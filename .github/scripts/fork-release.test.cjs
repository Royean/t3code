const assert = require("node:assert/strict");
const crypto = require("node:crypto");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const test = require("node:test");
const {
  resolveRelease,
  assertAllowedCommit,
  findReleaseByTag,
  writeChecksums,
  publishRelease,
} = require("./fork-release.cjs");

const source = {
  repository: "Royean/t3code",
  eventName: "push",
  ref: "refs/tags/v1.2.3-rc.1",
  runId: "42",
  date: new Date("2026-11-16T12:00:00Z"),
  baseVersion: "1.2.3",
};

test("release source accepts stable and prerelease versions and rejects unsafe sources", () => {
  assert.deepEqual(resolveRelease(source), {
    tag: "v1.2.3-rc.1",
    version: "1.2.3-rc.1",
    publish: true,
    prerelease: true,
  });
  assert.equal(resolveRelease({ ...source, ref: "refs/tags/v1.2.3" }).prerelease, false);
  for (const tag of [
    "v01.2.3",
    "v1.2.3-01",
    "v1.2.3-rc.01",
    "v1.2.3+meta",
    "v1.2",
    "v1.2.3-",
    "v1.2.3;echo bad",
  ]) {
    assert.throws(() => resolveRelease({ ...source, ref: `refs/tags/${tag}` }), /SemVer/);
  }
  for (const repository of ["pingdotgg/t3code", "other/t3code"]) {
    assert.throws(() => resolveRelease({ ...source, repository }), /configured fork/);
  }
  assert.throws(() => resolveRelease({ ...source, eventName: "pull_request" }), /allowed branch/);
  for (const branch of ["main", "feat/event-orchestration"]) {
    assert.deepEqual(
      resolveRelease({ ...source, eventName: "workflow_dispatch", ref: `refs/heads/${branch}` }),
      {
        tag: "",
        version: "1.2.3-preview.20261116.42",
        publish: false,
        prerelease: true,
      },
    );
  }
  assert.throws(
    () => resolveRelease({ ...source, eventName: "workflow_dispatch", ref: "refs/heads/unknown" }),
    /allowed branch/,
  );
});

function distribution(t) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "fork-release-test-"));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  for (const name of [
    "t3-1.2.3-darwin-arm64.tar.gz",
    "t3-1.2.3-linux-x64.tar.gz",
    "t3-1.2.3-win32-x64.zip",
    "t3-1.2.3-android-arm64.apk",
    "ANDROID-SIGNING.json",
    "BUILD-INFO.json",
    "T3-Code.dmg",
    "T3-Code.zip",
    "T3-Code.AppImage",
    "T3-Code.deb",
    "T3-Code.exe",
  ]) {
    fs.writeFileSync(path.join(directory, name), `fixture ${name}`);
  }
  return directory;
}

test("distribution requires every platform and hashes both ZIP archives and every asset", (t) => {
  const directory = distribution(t);
  for (const name of [
    "t3-1.2.3-linux-x64.tar.gz",
    "t3-1.2.3-win32-x64.zip",
    "t3-1.2.3-android-arm64.apk",
  ]) {
    const required = path.join(directory, name);
    fs.unlinkSync(required);
    assert.throws(() => writeChecksums(directory, "1.2.3"), /Missing release asset/);
    fs.writeFileSync(required, `fixture ${name}`);
  }
  const names = writeChecksums(directory, "1.2.3");
  const checksums = fs.readFileSync(path.join(directory, "SHA256SUMS"), "utf8").trim().split("\n");
  assert.equal(checksums.length, names.length - 1);
  for (const line of checksums) {
    const [sum, name] = line.split("  ");
    assert.equal(
      sum,
      crypto
        .createHash("sha256")
        .update(fs.readFileSync(path.join(directory, name)))
        .digest("hex"),
    );
  }
  assert.throws(() => writeChecksums(directory, "1.2.3"), /replace existing/);
});

test("distribution rejects duplicate desktop formats and symlinks", (t) => {
  const directory = distribution(t);
  const extra = path.join(directory, "extra.dmg");
  fs.writeFileSync(extra, "extra");
  assert.throws(() => writeChecksums(directory, "1.2.3"), /exactly one desktop/);
  fs.unlinkSync(extra);
  fs.symlinkSync("BUILD-INFO.json", path.join(directory, "link"));
  assert.throws(() => writeChecksums(directory, "1.2.3"), /regular files/);
});

function api({
  exists = false,
  failUpload = false,
  truncateUpload = false,
  moved = false,
  annotated = false,
  moveAfterUpload = false,
} = {}) {
  const state = { created: undefined, published: undefined, assets: [] };
  const github = {
    rest: {
      git: {
        async getRef() {
          return {
            data: {
              object: {
                type: annotated ? "tag" : "commit",
                sha: moved || (moveAfterUpload && state.assets.length) ? "other" : "checked",
              },
            },
          };
        },
        async getTag({ tag_sha }) {
          return { data: { object: { type: "commit", sha: tag_sha } } };
        },
      },
      repos: {
        async listReleases() {
          return { data: exists ? [{ id: 99, tag_name: "v1.2.3", draft: true }] : [] };
        },
        async createRelease(params) {
          state.created = params;
          return { data: { id: 10 } };
        },
        async uploadReleaseAsset({ name, data }) {
          if (failUpload) throw new Error("upload failed");
          const asset = { name, state: "uploaded", size: data.length - (truncateUpload ? 1 : 0) };
          state.assets.push(asset);
          return { data: asset };
        },
        async listReleaseAssets() {
          return { data: state.assets };
        },
        async updateRelease(params) {
          state.published = params;
          return { data: { html_url: "release-url" } };
        },
      },
    },
  };
  return { state, github };
}

test("publication refuses existing versions and moved tags, leaving failed uploads in draft", async (t) => {
  const directory = distribution(t);
  writeChecksums(directory, "1.2.3");
  const input = {
    repository: "Royean/t3code",
    tag: "v1.2.3",
    sha: "checked",
    prerelease: false,
    directory,
    body: "notes",
  };
  for (const scenario of [
    { exists: true },
    { moved: true },
    { failUpload: true },
    { truncateUpload: true },
    { moveAfterUpload: true },
  ]) {
    const { github, state } = api(scenario);
    await assert.rejects(publishRelease({ ...input, github }));
    assert.equal(state.published, undefined);
    if (state.created) assert.equal(state.created.draft, true);
  }
  const { github, state } = api({ annotated: true });
  assert.equal(await publishRelease({ ...input, github }), "release-url");
  assert.equal(state.assets.length, fs.readdirSync(directory).length);
  assert.equal(state.created.draft, true);
  assert.equal(state.published.draft, false);
  assert.equal(state.published.make_latest, "true");
});

test("release lookup finds old drafts with pagination and fails closed on API errors", async () => {
  const pages = [];
  const github = {
    rest: {
      repos: {
        async listReleases({ page }) {
          pages.push(page);
          return {
            data:
              page === 1
                ? Array.from({ length: 100 }, (_, i) => ({ tag_name: `v0.0.${i}` }))
                : [{ tag_name: "v1.2.3", draft: true }],
          };
        },
      },
    },
  };
  const input = { github, repository: "Royean/t3code", tag: "v1.2.3" };
  assert.equal((await findReleaseByTag(input)).draft, true);
  assert.deepEqual(pages, [1, 2]);
  github.rest.repos.listReleases = async () => {
    throw Object.assign(new Error("forbidden"), { status: 403 });
  };
  await assert.rejects(findReleaseByTag(input), /forbidden/);
});

test("source ancestry uses captured remote commits and rejects ahead or divergent candidates", async () => {
  const branchCommits = { main: "1".repeat(40), "feat/event-orchestration": "2".repeat(40) };
  const calls = [];
  let statuses = ["behind", "ahead"];
  const github = {
    rest: {
      repos: {
        async compareCommitsWithBasehead({ basehead }) {
          calls.push(basehead);
          return { data: { status: statuses.shift() } };
        },
      },
    },
  };
  const input = { github, repository: "Royean/t3code", sha: "candidate", branchCommits };
  assert.equal(await assertAllowedCommit(input), "feat/event-orchestration");
  assert.deepEqual(calls, [
    `candidate...${branchCommits.main}`,
    `candidate...${branchCommits["feat/event-orchestration"]}`,
  ]);
  statuses = ["behind", "diverged"];
  await assert.rejects(assertAllowedCommit(input), /Commit must belong/);
  await assert.rejects(
    assertAllowedCommit({ ...input, branchCommits: { main: "forged-ref" } }),
    /Missing trusted ref/,
  );
});
