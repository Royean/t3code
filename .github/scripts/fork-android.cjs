const ANDROID_PACKAGE = "com.royean.t3code";

function patchExpoConfig(source, { version, versionCode }) {
  if (
    !/^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$/.test(version) ||
    !Number.isSafeInteger(versionCode) ||
    versionCode < 1 ||
    versionCode > 2100000000
  ) {
    throw new Error("Invalid Android release version.");
  }
  const marker = "export default config;";
  if (source.split(marker).length !== 2)
    throw new Error("Expo config export changed; review fork overrides.");
  return source.replace(
    marker,
    [
      'config.name = "T3 Code Fork";',
      'config.scheme = "t3code-fork";',
      `config.version = ${JSON.stringify(version)};`,
      `config.runtimeVersion = ${JSON.stringify(version)};`,
      `config.android = { ...config.android, package: "${ANDROID_PACKAGE}", versionCode: ${versionCode} };`,
      // Fork builds must never receive JavaScript from the upstream EAS project.
      "config.updates = { enabled: false };",
      marker,
    ].join("\n"),
  );
}

function disableDebugSigning(source) {
  const releaseSigning = /(release\s*\{[\s\S]*?)signingConfig signingConfigs\.debug/;
  if (!releaseSigning.test(source)) throw new Error("Generated release signing config changed.");
  return source.replace(releaseSigning, "$1signingConfig null");
}

function verifyApkMetadata({ badging, entries, version }) {
  const metadata = /^package: name='([^']+)' versionCode='(\d+)' versionName='([^']+)'/m.exec(
    badging,
  );
  if (!metadata || metadata[1] !== ANDROID_PACKAGE || metadata[3] !== version) {
    throw new Error("APK package or version does not match the fork release.");
  }
  if (/^application-debuggable/m.test(badging)) throw new Error("APK must not be debuggable.");
  if (!/^launchable-activity:/m.test(badging)) throw new Error("APK has no launchable activity.");
  if (!entries.some(({ name, size }) => name === "assets/index.android.bundle" && size > 1024)) {
    throw new Error("APK must embed a nonempty JavaScript/Hermes bundle to run without Metro.");
  }
  if (!entries.some(({ name }) => /^lib\/arm64-v8a\/.*\.so$/.test(name))) {
    throw new Error("APK must include the Android arm64 native runtime.");
  }
  if (entries.some(({ name }) => /^lib\/(?!arm64-v8a\/)[^/]+\/.*\.so$/.test(name))) {
    throw new Error("Unexpected APK architecture.");
  }
  return {
    package: metadata[1],
    version: metadata[3],
    versionCode: Number(metadata[2]),
    architecture: "arm64-v8a",
    debuggable: false,
    embeddedJavaScript: true,
    updates: "manual",
  };
}

module.exports = { ANDROID_PACKAGE, patchExpoConfig, disableDebugSigning, verifyApkMetadata };
