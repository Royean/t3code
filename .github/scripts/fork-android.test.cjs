const assert = require("node:assert/strict");
const test = require("node:test");
const { patchExpoConfig, disableDebugSigning, verifyApkMetadata } = require("./fork-android.cjs");

test("fork Android prebuild uses its own identity, embedded runtime version and disabled upstream OTA", () => {
  const source = "const config = {};\nexport default config;";
  const patched = patchExpoConfig(source, { version: "0.1.0-rc.1", versionCode: 3 });
  const config = new Function(patched.replace("export default config;", "return config;"))();
  assert.equal(config.android.package, "com.royean.t3code");
  assert.equal(config.android.versionCode, 3);
  assert.equal(config.version, "0.1.0-rc.1");
  assert.equal(config.runtimeVersion, config.version);
  assert.deepEqual(config.updates, { enabled: false });
  assert.throws(
    () => patchExpoConfig(source, { version: "bad\nversion", versionCode: 3 }),
    /Invalid/,
  );
  assert.throws(
    () => patchExpoConfig("export default {};", { version: "1.0.0", versionCode: 1 }),
    /export changed/,
  );
  const gradle =
    "debug { signingConfig signingConfigs.debug }\nrelease { signingConfig signingConfigs.debug }";
  assert.equal(
    disableDebugSigning(gradle),
    "debug { signingConfig signingConfigs.debug }\nrelease { signingConfig null }",
  );
  assert.throws(() => disableDebugSigning("release {}"), /signing config changed/);
});

test("standalone APK verification rejects debug, missing JS, wrong identity and absent native runtime", () => {
  const input = {
    version: "0.1.0-rc.1",
    badging:
      "package: name='com.royean.t3code' versionCode='3' versionName='0.1.0-rc.1'\nlaunchable-activity: name='com.royean.t3code.MainActivity'",
    entries: [
      { name: "assets/index.android.bundle", size: 10000 },
      { name: "lib/arm64-v8a/libreactnative.so", size: 10000 },
    ],
  };
  assert.equal(verifyApkMetadata(input).embeddedJavaScript, true);
  assert.throws(
    () => verifyApkMetadata({ ...input, badging: input.badging + "\napplication-debuggable" }),
    /debuggable/,
  );
  assert.throws(
    () =>
      verifyApkMetadata({ ...input, badging: input.badging.replace("com.royean", "com.t3tools") }),
    /package or version/,
  );
  assert.throws(() => verifyApkMetadata({ ...input, version: "0.1.1" }), /package or version/);
  assert.throws(
    () => verifyApkMetadata({ ...input, entries: input.entries.slice(1) }),
    /JavaScript/,
  );
  assert.throws(
    () => verifyApkMetadata({ ...input, entries: input.entries.slice(0, 1) }),
    /native runtime/,
  );
  assert.throws(
    () =>
      verifyApkMetadata({
        ...input,
        entries: [...input.entries, { name: "lib/x86_64/libreactnative.so", size: 10000 }],
      }),
    /Unexpected APK architecture/,
  );
});
