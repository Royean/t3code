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
  for 14 days after the full checks and both platform smoke tests pass.

The workflow builds macOS Apple Silicon DMG/ZIP and Linux x64 AppImage/DEB desktop
packages, plus standalone CLI archives for both platforms. Each CLI archive
includes the web client and native runtime dependencies; extract it and run its
`t3` executable without installing Node. Release assets include `SHA256SUMS` and
`BUILD-INFO.json` with the source commit, version, platforms, build time and run URL.
On Linux, run `sha256sum -c SHA256SUMS`; on macOS, use `shasum -a 256 -c SHA256SUMS`.

Every tag build runs this repository's complete CI on the same commit. Publishing
waits for CI, both native CLI archive smoke tests and both packaged desktop launch
checks. Files upload to a draft first; the draft becomes public only after all
uploads are verified. Existing releases, including failed drafts, are never
overwritten. Inspect a failed draft and use a fresh version for another release.
The tag is checked again before publication so moved tags cannot publish stale
artifacts.
The publishing job loads its release helper from the exact `main` revision
captured before building; candidate-branch code only runs in read-only jobs.

No additional secrets are required; only the repository's `GITHUB_TOKEN` writes
the release. These are unsigned personal builds: the macOS CLI is signed ad hoc,
and desktop packages have no Developer ID signature or Apple notarization.
Gatekeeper may require approval through macOS Privacy & Security. Desktop update
configuration points at this fork; use manual installation and upgrades for these
unsigned builds. The CLI's built-in release discovery still queries upstream, so
download this fork's CLI archive manually when upgrading.

The builds do not inject the upstream relay or Clerk configuration, publish to npm,
deploy any service, or create mobile store/EAS builds. Windows, Intel macOS and
Linux arm64 packages are outside this workflow's current platform coverage.
