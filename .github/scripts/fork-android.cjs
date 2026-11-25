const { createHash, X509Certificate } = require("node:crypto");

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
      'config.name = "t4code";',
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

function verifyApkSigningCertificates({ output, expectedCertificate }) {
  // SDK 37 changed human-readable signer labels. Compare every printed X.509
  // certificate with keytool's exported DER instead of relying on those labels.
  const certificates =
    output.match(/-----BEGIN CERTIFICATE-----[\s\S]*?-----END CERTIFICATE-----/g) ?? [];
  const beginCount = output.match(/-----BEGIN CERTIFICATE-----/g)?.length ?? 0;
  const endCount = output.match(/-----END CERTIFICATE-----/g)?.length ?? 0;
  if (
    !certificates.length ||
    beginCount !== certificates.length ||
    endCount !== certificates.length
  ) {
    throw new Error("APK signing certificate output is missing or incomplete.");
  }
  let expected;
  try {
    expected = new X509Certificate(expectedCertificate).raw;
    for (const certificate of certificates) {
      if (!new X509Certificate(certificate).raw.equals(expected)) {
        throw new Error("Unexpected signing certificate.");
      }
    }
  } catch (cause) {
    throw new Error("APK signing certificate does not match the configured keystore.", { cause });
  }
  return createHash("sha256").update(expected).digest("hex");
}

module.exports = {
  ANDROID_PACKAGE,
  patchExpoConfig,
  disableDebugSigning,
  verifyApkMetadata,
  verifyApkSigningCertificates,
};
