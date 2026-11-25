# Personal fork builds

`Royean/t4code` uses standard GitHub-hosted runners. The upstream release and
deployment workflows retain their repository guards and credentials.

- `CI` runs the existing full checks and desktop/web build on pushes to `main`
  and `feat/event-orchestration`, and affected checks on pull requests.
- `Fork Release` runs on `v*` tag pushes. Only `vX.Y.Z` and valid SemVer prerelease
  tags, such as `v0.1.0-rc.1`, may publish. The tagged commit must be contained in
  `main` or `feat/event-orchestration`.
- Dispatch `Fork Release` from either allowed branch to test packaging without
  creating a tag or release. Its `X.Y.Z-preview.YYYYMMDD.RUN_ID` version has no
  desktop update feed. A complete distribution is available as an Actions artifact
  for 14 days after the full checks and every required platform build pass.

The workflow builds macOS Apple Silicon DMG/ZIP and Linux x64 AppImage/DEB desktop
packages, plus standalone CLI archives for both platforms. On macOS, install
**t4code.app** alongside the official desktop app. t4code uses its own
`~/.t3-pro` data directory and starts with a fresh profile. Build it locally with
`T3CODE_DESKTOP_MAC_PRO=true T3CODE_SELF_HOSTED_ONLY=1 vp run dist:desktop:dmg:arm64`.
Fork packages use the self-hosted T3 Account at `https://t3.vivid-dream.online`. Each CLI archive
includes the web client and native runtime dependencies; extract it and run its
`t3` executable without installing Node. Release assets include `SHA256SUMS` and
`BUILD-INFO.json` with the source commit, version, platforms, build time and run URL.
On Linux, run `sha256sum -c SHA256SUMS`; on macOS, use `shasum -a 256 -c SHA256SUMS`.
Windows x64 builds add an unsigned NSIS installer and standalone CLI ZIP. The
desktop installer embeds the same Linux x64 archive for WSL environments. Both
the archived Windows CLI and the packaged Windows desktop run on the Windows
runner before the assets are accepted.

Android builds run Expo production prebuild and Gradle `assembleRelease`, producing
an arm64 APK with its JavaScript/Hermes bundle embedded. The APK is not a Metro
development client. Its package is `com.royean.t3code`, display name is `t4code`, and upstream EAS over-the-air updates are disabled. It can coexist with the
official app. APK validation rejects debug builds, missing embedded JavaScript,
incorrect package/version and missing arm64 native libraries. The release's
`ANDROID-SIGNING.json` records those checks and the public certificate fingerprint.

Every tag build runs this repository's complete CI on the same commit. Publishing
waits for CI, all three native CLI archive smoke tests, three packaged desktop
launch checks, and the signed, validated Android APK. Files upload to a draft first;
the draft becomes public only after all
uploads are verified. Existing releases, including failed drafts, are never
overwritten. Inspect a failed draft and use a fresh version for another release.
The tag is checked again before publication so moved tags cannot publish stale
artifacts.
Preparation captures the actual GitHub `main` and feature branch commit IDs before
loading repository code. The source policy and publishing helper come from that
exact `main` revision. Source ancestry is checked against those captured remote
commits through the GitHub API. Release lookup pages through all releases so old
drafts also reserve their versions. Candidate build code runs without publishing
tokens or Android signing secrets.

The repository's `GITHUB_TOKEN` writes the release. Desktop builds need no extra
secrets. These are unsigned personal desktop builds: the macOS CLI is signed ad hoc,
and desktop packages have no Developer ID signature or Apple notarization.
Gatekeeper may require approval through macOS Privacy & Security. Desktop update
configuration points at this fork; use manual installation and upgrades for these
unsigned builds. The CLI's built-in release discovery still queries upstream, so
download this fork's CLI archive manually when upgrading.
Windows binaries have no Authenticode signature and may trigger SmartScreen.

Android requires two repository secrets: `FORK_ANDROID_KEYSTORE_BASE64` contains a
persistent PKCS12 keystore; `FORK_ANDROID_KEYSTORE_PASSWORD` supplies its store and
key password. The key alias is `t3code-fork`. Keep and back up this key: subsequent
APK upgrades must use the same certificate. The generated Gradle debug signing
configuration is removed. A separate signing job checks out trusted `main` tools,
signs the unsigned artifact, verifies the APK signature and compares its
certificate with the configured keystore. It never executes candidate Gradle or
JavaScript with signing credentials.

The Android fork delivers notifications over HTTPS from the account service. Upgrade
that service and verify background receipt and notification links from the signed APK
on a physical Android device before tagging.
The workflow does not publish to npm, deploy upstream services, use EAS cloud
builders or publish mobile store builds. Intel macOS and Linux arm64 packages are
outside this workflow's current platform coverage.

After a tag release is published, `deploy` verifies this run's exact Linux CLI
archive against the checksum produced by the trusted publishing job and streams
it over SSH to the Tencent server. It executes no candidate repository scripts.
Dispatch builds never deploy. Configure `DEPLOY_HOST`, `DEPLOY_USER`,
`DEPLOY_SSH_KEY` and `DEPLOY_KNOWN_HOSTS` repository secrets after installing the
server-side deployment helper. The dedicated non-root SSH identity must be bound
to the `t3code TAG SHA SHA256` forced-command protocol; no SCP, SFTP, interactive
shell or root SSH key is used. Host key verification is mandatory.

The server helper owns idle checks, backup, runtime compatibility checks, service
restart and rollback. It preserves the existing T3 data and loopback endpoint.
The Linux runtime includes native dependencies and a Rust resource monitor, so
the server must verify compatibility with its OpenCloudOS/glibc version before
switching. A failed deployment leaves the GitHub Release published and fails the
deployment job; inspect the server result before rerunning that job.
