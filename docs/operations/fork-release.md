# Personal fork builds

`Royean/t3code` uses standard GitHub-hosted runners. The upstream release and
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
packages, plus standalone CLI archives for both platforms. Each CLI archive
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
development client. Its package is `com.royean.t3code`, display name is `T3 Code
Fork`, and upstream EAS over-the-air updates are disabled. It can coexist with the
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

Android uses the existing public Clerk/relay identifiers in `.env.example` for T3
account connections. Native Google sign-in and push notifications may require
registration of the fork package and certificate; they require device validation.
The workflow does not publish to npm, deploy upstream services, use EAS cloud
builders or publish mobile store builds. Intel macOS and Linux arm64 packages are
outside this workflow's current platform coverage.
