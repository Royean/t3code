import { describe, expect, it } from "@effect/vitest";
import { decodeConfig } from "./config.ts";

const input = {
  databasePath: "/var/lib/t3-connect/state.sqlite",
  account: {
    accountId: "owner",
    username: "owner",
    passwordHash:
      "scrypt$32768$8$1$ca33483ce1102b6d2057afec1a065ced$935b09158b33db6019b3441ef559389dd9ffdb09409da5ac46829d640d7c58af",
  },
  environments: [
    {
      environmentId: "office",
      label: "Office",
      origin: "https://office.example.com",
      pairingOrigin: "http://127.0.0.1:3773",
      credential: "private-environment-credential",
    },
  ],
};

describe("desktop CORS origins", () => {
  it.each(["t3code://app", "t3code-dev://app"])(
    "preserves %s as a distinct allowed origin",
    (origin) => {
      expect(
        decodeConfig({ ...input, allowedOrigins: [origin, `${origin}/`] }).allowedOrigins,
      ).toEqual([origin, origin]);
    },
  );

  it.each([
    "t3code://attacker",
    "t3code://app.attacker.example",
    "t3code://user@app",
    "t3code://app:1234",
    "t3code://app/path",
    "t3code://app?query=1",
    "t3code://app#fragment",
    "null",
  ])("rejects %s", (origin) => {
    expect(() => decodeConfig({ ...input, allowedOrigins: [origin] })).toThrow();
  });

  it("keeps custom schemes out of environment fetch targets", () => {
    for (const key of ["origin", "pairingOrigin"] as const) {
      expect(() =>
        decodeConfig({
          ...input,
          environments: [{ ...input.environments[0], [key]: "t3code://app" }],
        }),
      ).toThrow();
    }
  });
});
