const assert = require("node:assert/strict");
const test = require("node:test");
const { X509Certificate, createHash } = require("node:crypto");
const {
  patchExpoConfig,
  disableDebugSigning,
  verifyApkMetadata,
  verifyApkSigningCertificates,
} = require("./fork-android.cjs");

test("fork Android prebuild uses its own identity, embedded runtime version and disabled upstream OTA", () => {
  const source = "const config = {};\nexport default config;";
  const patched = patchExpoConfig(source, { version: "0.1.0-rc.1", versionCode: 3 });
  const config = new Function(patched.replace("export default config;", "return config;"))();
  assert.equal(config.name, "t4code");
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

// Public certificates generated for these tests; their private keys were discarded.
const SIGNING_CERTIFICATE = `-----BEGIN CERTIFICATE-----
MIIDMTCCAhmgAwIBAgIUH0nDm0QeNcA1OY6Trm1j7W9Y2TkwDQYJKoZIhvcNAQEL
BQAwKDEmMCQGA1UEAwwdRm9ya1NpZ25hdHVyZUZpeHR1cmUtZXhwZWN0ZWQwHhcN
MjYxMTE2MDEzODM5WhcNMjYxMTE3MDEzODM5WjAoMSYwJAYDVQQDDB1Gb3JrU2ln
bmF0dXJlRml4dHVyZS1leHBlY3RlZDCCASIwDQYJKoZIhvcNAQEBBQADggEPADCC
AQoCggEBAJQx2DaeQ3EWbNt+0c2xWilXoN8Jm47UdvctCex+xhUeIaHluR1/jX7N
7tbzG+cWlWAKN1olo2oJBY0DWUNe0fOULDxwgBpiDSTGqslvf0LVw0womu1JXr+x
VRRgeXk03+pcTVvVAFjOFid+DrKstjJB/l/FsK7xY23/DcLGaggLwiqCgie+utuz
cJ3AXEqsOY8q25aBP8omhVYCuDploU9eUQ3tGwNY6htQ8T2HaYFOvvQdin6oYtdN
+jDFzWMhUKuLe/hvKyzv1wJH6s9yDIiPexQ9oIUHQ2iaVnx6cTwcQwE624yIJU5w
5X5Qt2ICwqJxLMX1nLG8FiSmTl34c80CAwEAAaNTMFEwHQYDVR0OBBYEFAjVTw4R
U4b3HFQI75APXCsWcV0RMB8GA1UdIwQYMBaAFAjVTw4RU4b3HFQI75APXCsWcV0R
MA8GA1UdEwEB/wQFMAMBAf8wDQYJKoZIhvcNAQELBQADggEBACKuCt8c2t4qkILc
KEOGLYwJVoa83ZdnPGV2lGEzhXZoBguuIaI3XXTiqtzYGCYKCmi5le7qHJ6sqfmz
dtPvyRXJGD/vsh1H9aGM9AISmDFKa1fYPNzb5mupaNwxIMen2hc3tyLh2xyZKHWf
UKj49G+Aq/d7JTO5IPh1TKvFPUH5rrRG+OdAiTlByt4wODkGme2BfjAxy4T2BBse
EzeVv0Qv3HVJQQ/M/hDcyE7Q8WeBOGVa1KCBM88qJ1oRBZjRnDX6Z7q6gyoY+Z7t
ykq3Hyo6c6HzT+BhilkQ/6tkfd0wtCU6MsNe2h7wKhanhHzxRlerX+o6F7VyckvR
GXu3mgo=
-----END CERTIFICATE-----
`;
const OTHER_CERTIFICATE = `-----BEGIN CERTIFICATE-----
MIIDKzCCAhOgAwIBAgIUMusv1H/L6hWlTfawVeoeuUt/u0YwDQYJKoZIhvcNAQEL
BQAwJTEjMCEGA1UEAwwaRm9ya1NpZ25hdHVyZUZpeHR1cmUtb3RoZXIwHhcNMjYx
MTE2MDEzODM5WhcNMjYxMTE3MDEzODM5WjAlMSMwIQYDVQQDDBpGb3JrU2lnbmF0
dXJlRml4dHVyZS1vdGhlcjCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEB
AMZIL6/eM29VaLKJ81h+pFU+MHG17YgqbSTDtRpUBanIasMSQNXL6IQTBAF7UUAh
QZ1FiuII70gR1v6aXwgTy919oD3r84sj5cVbyTj7Cemgu5vU9I1qNh1LERwgPHMz
VBFlBZ2XPAamfx9wZ5q1GoRGYmK3EsZ7Mojg+eHhckp6L6MOsNppkzXKSKWQEXGh
C1JrB0YM2nRQO5yurA4XrwvFlmaPltZ3/dNCUh390dYX/jCivIHDetM2SEWm1YnS
tcqUNr//1XWUWkjz545zWNGJRsuPnSUB6EiUVBw5qcMsvlYHEFx2TTl8Wz23jv8x
SrCMOC4kqCMFzPJWX6/qotkCAwEAAaNTMFEwHQYDVR0OBBYEFFqQh31M8hE+a9z5
vxMhzUZAZYwlMB8GA1UdIwQYMBaAFFqQh31M8hE+a9z5vxMhzUZAZYwlMA8GA1Ud
EwEB/wQFMAMBAf8wDQYJKoZIhvcNAQELBQADggEBAGEjcDbvREdqtxWMeSvFdkZd
iRX5rwomfJfpEsPILGH4fzuSeSL2dgjbgdVlJw7npmDNTC5BAZsFhQvSk23IPakP
mmaPCcyjp9LrFA6jF/wFEErvfFzEHdBSbLc6alq/TAAWLCFFydv8dGXWCkG3AFQ7
Z2M2NljYIJvSMCQpry+SdJXGSA9TpY8Fk6wkzMBy+Wcv49icjGmXTxWp655h0lv6
pJA6iY7tqr3xGVZX5ntQo/AZ4HD9Gm2H1Rfj7w2smeTODdnEjt8SXWERmSt7lZA7
E0dIzz/SqBcdbpEPYCs8eB6bWcCs0ec3K7Olcy3n93Q13GAuBNDlaVb/EUegldU=
-----END CERTIFICATE-----
`;

test("APK certificate matching handles SDK 37 scheme labels and legacy labels using verified PEM", () => {
  const expectedCertificate = new X509Certificate(SIGNING_CERTIFICATE).raw;
  const expectedHash = createHash("sha256").update(expectedCertificate).digest("hex");
  for (const label of [
    "Signer #1",
    "V2 Signer:",
    "V3.0 Signer:",
    "Signer (minSdkVersion=24, maxSdkVersion=32)",
  ]) {
    assert.equal(
      verifyApkSigningCertificates({
        output: `${label} certificate SHA-256 digest: ${expectedHash}\n${SIGNING_CERTIFICATE}`,
        expectedCertificate,
      }),
      expectedHash,
    );
  }
  assert.equal(
    verifyApkSigningCertificates({
      output: `V2 Signer:\n${SIGNING_CERTIFICATE}V3 Signer:\n${SIGNING_CERTIFICATE}`,
      expectedCertificate,
    }),
    expectedHash,
  );
});

test("APK certificate matching rejects missing, malformed, truncated and unexpected signers", () => {
  const expectedCertificate = new X509Certificate(SIGNING_CERTIFICATE).raw;
  for (const output of [
    "Signer #1 certificate SHA-256 digest: " +
      createHash("sha256").update(expectedCertificate).digest("hex"),
    OTHER_CERTIFICATE,
    SIGNING_CERTIFICATE + OTHER_CERTIFICATE,
    "-----BEGIN CERTIFICATE-----\nZm9v\n-----END CERTIFICATE-----",
    SIGNING_CERTIFICATE + "-----BEGIN CERTIFICATE-----\nZm9v",
    SIGNING_CERTIFICATE + "-----END CERTIFICATE-----",
  ]) {
    assert.throws(
      () => verifyApkSigningCertificates({ output, expectedCertificate }),
      /^Error: APK signing certificate/,
    );
  }
});
