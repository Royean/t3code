const crypto = require("node:crypto");
const fs = require("node:fs");
const path = require("node:path");

const FORK_REPOSITORY = "Royean/t3code";
const ALLOWED_BRANCHES = ["main", "feat/event-orchestration"];
const SEMVER_TAG =
  /^v(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*)(?:\.(?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*))*))?$/;

function resolveRelease({ repository, eventName, ref, runId, date, baseVersion }) {
  if (repository !== FORK_REPOSITORY)
    throw new Error("This workflow only builds the configured fork.");
  if (eventName === "push" && ref.startsWith("refs/tags/")) {
    const tag = ref.slice("refs/tags/".length);
    const match = SEMVER_TAG.exec(tag);
    if (!match)
      throw new Error(
        "Release tags must be vX.Y.Z or a SemVer prerelease, without build metadata.",
      );
    return { tag, version: tag.slice(1), publish: true, prerelease: Boolean(match[4]) };
  }
  if (
    eventName !== "workflow_dispatch" ||
    !ALLOWED_BRANCHES.some((branch) => ref === `refs/heads/${branch}`)
  ) {
    throw new Error("Build-only dispatches must select an allowed branch.");
  }
  if (!/^\d+\.\d+\.\d+$/.test(baseVersion) || !/^\d+$/.test(String(runId))) {
    throw new Error("Invalid build version or run ID.");
  }
  const day = date.toISOString().slice(0, 10).replaceAll("-", "");
  return {
    tag: "",
    version: `${baseVersion}-preview.${day}.${runId}`,
    publish: false,
    prerelease: true,
  };
}

async function assertAllowedCommit({ github, repository, sha, branchCommits }) {
  if (repository !== FORK_REPOSITORY) throw new Error("Invalid release repository.");
  const [owner, repo] = repository.split("/");
  for (const branch of ALLOWED_BRANCHES) {
    const head = branchCommits[branch];
    if (!/^[0-9a-f]{40}$/.test(head ?? "")) throw new Error(`Missing trusted ref: ${branch}`);
    const { data } = await github.rest.repos.compareCommitsWithBasehead({
      owner,
      repo,
      basehead: `${sha}...${head}`,
    });
    if (data.status === "identical" || data.status === "ahead") return branch;
  }
  throw new Error("Commit must belong to main or feat/event-orchestration.");
}

async function resolveSourceCommit({ github, repository, eventName, ref, sha }) {
  if (eventName !== "push" || !ref.startsWith("refs/tags/")) return sha;
  const [owner, repo] = repository.split("/");
  const { data } = await github.rest.git.getRef({ owner, repo, ref: ref.slice("refs/".length) });
  let object = data.object;
  const tagSha = object.sha;
  for (let depth = 0; object.type === "tag" && depth < 8; depth++) {
    object = (await github.rest.git.getTag({ owner, repo, tag_sha: object.sha })).data.object;
  }
  if (object.type !== "commit" || (sha !== tagSha && sha !== object.sha)) {
    throw new Error("Release tag moved away from the triggering commit.");
  }
  return object.sha;
}

async function findReleaseByTag({ github, repository, tag }) {
  const [owner, repo] = repository.split("/");
  // Listing with the repository token includes drafts; getReleaseByTag only
  // promises published releases. Keep walking so an old draft reserves its tag.
  for (let page = 1; ; page++) {
    const { data } = await github.rest.repos.listReleases({ owner, repo, page, per_page: 100 });
    const match = data.find((release) => release.tag_name === tag);
    if (match) return match;
    if (data.length < 100) return undefined;
  }
}

function writeChecksums(directory, version) {
  const entries = fs.readdirSync(directory, { withFileTypes: true });
  if (entries.some((entry) => !entry.isFile() || !/^[0-9A-Za-z._+-]+$/.test(entry.name))) {
    throw new Error("Release assets must be regular files with safe, unique names.");
  }
  const names = entries.map((entry) => entry.name).sort();
  const required = [
    `t3-${version}-darwin-arm64.tar.gz`,
    `t3-${version}-linux-x64.tar.gz`,
    `t3-${version}-win32-x64.zip`,
    `t3-${version}-android-arm64.apk`,
    "ANDROID-SIGNING.json",
    "BUILD-INFO.json",
  ];
  for (const name of required) {
    if (!names.includes(name)) throw new Error(`Missing release asset: ${name}`);
  }
  for (const extension of [".dmg", ".zip", ".AppImage", ".deb", ".exe", ".apk"]) {
    if (
      names.filter((name) => name.endsWith(extension) && name !== `t3-${version}-win32-x64.zip`)
        .length !== 1
    ) {
      throw new Error(`Expected exactly one desktop ${extension} asset.`);
    }
  }
  if (names.includes("SHA256SUMS")) throw new Error("Refusing to replace existing checksums.");
  const lines = names.map((name) => {
    const bytes = fs.readFileSync(path.join(directory, name));
    if (bytes.length === 0) throw new Error(`Empty release asset: ${name}`);
    return `${crypto.createHash("sha256").update(bytes).digest("hex")}  ${name}\n`;
  });
  fs.writeFileSync(path.join(directory, "SHA256SUMS"), lines.join(""));
  return [...names, "SHA256SUMS"].sort();
}

// A draft is the only visible state until every upload and its size have been
// verified. A failed run leaves a draft for inspection; reruns never overwrite it.
async function publishRelease({ github, repository, tag, sha, prerelease, directory, body }) {
  if (repository !== FORK_REPOSITORY || !SEMVER_TAG.test(tag))
    throw new Error("Invalid release destination.");
  const [owner, repo] = repository.split("/");
  async function verifyTag() {
    const { data: ref } = await github.rest.git.getRef({ owner, repo, ref: `tags/${tag}` });
    let object = ref.object;
    for (let depth = 0; object.type === "tag" && depth < 8; depth++) {
      object = (await github.rest.git.getTag({ owner, repo, tag_sha: object.sha })).data.object;
    }
    if (object.type !== "commit" || object.sha !== sha) {
      throw new Error("Release tag moved away from the checked commit.");
    }
  }
  await verifyTag();
  if (await findReleaseByTag({ github, repository, tag })) {
    throw new Error(`Release ${tag} already exists; use a new version.`);
  }
  const { data: release } = await github.rest.repos.createRelease({
    owner,
    repo,
    tag_name: tag,
    target_commitish: sha,
    name: `T3 Code fork ${tag}`,
    body,
    draft: true,
    prerelease,
  });
  const names = fs.readdirSync(directory).sort();
  for (const name of names) {
    const bytes = fs.readFileSync(path.join(directory, name));
    const { data: asset } = await github.rest.repos.uploadReleaseAsset({
      owner,
      repo,
      release_id: release.id,
      name,
      data: bytes,
      headers: { "content-type": "application/octet-stream", "content-length": bytes.length },
    });
    if (asset.size !== bytes.length || asset.state !== "uploaded") {
      throw new Error(`Incomplete upload: ${name}. Release remains a draft.`);
    }
  }
  const { data: assets } = await github.rest.repos.listReleaseAssets({
    owner,
    repo,
    release_id: release.id,
    per_page: 100,
  });
  if (assets.length !== names.length || assets.some((asset) => !names.includes(asset.name))) {
    throw new Error("Release asset set did not match the verified build. Release remains a draft.");
  }
  await verifyTag();
  const { data: published } = await github.rest.repos.updateRelease({
    owner,
    repo,
    release_id: release.id,
    draft: false,
    make_latest: prerelease ? "false" : "true",
  });
  return published.html_url;
}

module.exports = {
  ALLOWED_BRANCHES,
  FORK_REPOSITORY,
  resolveRelease,
  assertAllowedCommit,
  resolveSourceCommit,
  findReleaseByTag,
  writeChecksums,
  publishRelease,
};
