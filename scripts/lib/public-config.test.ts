// @effect-diagnostics nodeBuiltinImport:off - Tests exercise root env file precedence directly.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { afterEach, describe, expect, it } from "vite-plus/test";

import { loadRepoEnv } from "./public-config.ts";

const temporaryDirectories: string[] = [];

afterEach(() => {
  for (const directory of temporaryDirectories.splice(0)) {
    NodeFS.rmSync(directory, { recursive: true, force: true });
  }
});

describe("loadRepoEnv", () => {
  it("supplies public T3 Connect defaults and the self-hosted origin only for Mac Pro builds", () => {
    const repoRoot = makeTemporaryDirectory();
    NodeFS.writeFileSync(
      NodePath.join(repoRoot, ".env.example"),
      "T3CODE_CLERK_PUBLISHABLE_KEY=pk_example\nT3CODE_CLERK_JWT_TEMPLATE=example\nT3CODE_RELAY_URL=https://official.example.test\nT3CODE_MOBILE_OTLP_TRACES_TOKEN=ignored\n",
    );

    const normal = loadRepoEnv({ baseEnv: {}, repoRoot });
    expect(normal.VITE_CLERK_PUBLISHABLE_KEY).toBeUndefined();
    expect(normal.VITE_T3CODE_SELF_HOSTED_CONNECT_URL).toBeUndefined();
    const pro = loadRepoEnv({
      baseEnv: { T3CODE_DESKTOP_MAC_PRO: "true", T3CODE_CLERK_JWT_TEMPLATE: "override" },
      repoRoot,
    });
    expect(pro).toMatchObject({
      VITE_CLERK_PUBLISHABLE_KEY: "pk_example",
      VITE_CLERK_JWT_TEMPLATE: "override",
      VITE_T3CODE_RELAY_URL: "https://official.example.test",
      VITE_T3CODE_SELF_HOSTED_CONNECT_URL: "https://t3.vivid-dream.online",
    });
    expect(pro.T3CODE_MOBILE_OTLP_TRACES_TOKEN).toBeUndefined();
  });

  it("applies process, root local, and root precedence in that order", () => {
    const repoRoot = makeTemporaryDirectory();
    NodeFS.writeFileSync(
      NodePath.join(repoRoot, ".env"),
      "T3CODE_CLERK_PUBLISHABLE_KEY=pk_root\nT3CODE_CLERK_JWT_TEMPLATE=template_root\nT3CODE_CLERK_CLI_OAUTH_CLIENT_ID=oauth_root\nT3CODE_RELAY_URL=https://root.example.test\n",
    );
    NodeFS.writeFileSync(
      NodePath.join(repoRoot, ".env.local"),
      "T3CODE_CLERK_PUBLISHABLE_KEY=pk_local\nT3CODE_CLERK_JWT_TEMPLATE=template_local\nT3CODE_CLERK_CLI_OAUTH_CLIENT_ID=oauth_local\nT3CODE_RELAY_URL=https://local.example.test\n",
    );

    expect(loadRepoEnv({ baseEnv: {}, repoRoot }).T3CODE_RELAY_URL).toBe(
      "https://local.example.test",
    );
    expect(
      loadRepoEnv({
        baseEnv: {
          T3CODE_CLERK_PUBLISHABLE_KEY: "pk_ci",
          T3CODE_CLERK_JWT_TEMPLATE: "template_ci",
          T3CODE_CLERK_CLI_OAUTH_CLIENT_ID: "oauth_ci",
          T3CODE_RELAY_URL: "https://ci.example.test",
        },
        repoRoot,
      }),
    ).toMatchObject({
      T3CODE_CLERK_PUBLISHABLE_KEY: "pk_ci",
      T3CODE_CLERK_CLI_OAUTH_CLIENT_ID: "oauth_ci",
      VITE_CLERK_PUBLISHABLE_KEY: "pk_ci",
      EXPO_PUBLIC_CLERK_PUBLISHABLE_KEY: "pk_ci",
      T3CODE_CLERK_JWT_TEMPLATE: "template_ci",
      VITE_CLERK_JWT_TEMPLATE: "template_ci",
      EXPO_PUBLIC_CLERK_JWT_TEMPLATE: "template_ci",
      T3CODE_RELAY_URL: "https://ci.example.test",
      VITE_T3CODE_RELAY_URL: "https://ci.example.test",
    });
  });

  it("builds the self-hosted client without public Clerk or official relay configuration", () => {
    const repoRoot = makeTemporaryDirectory();
    NodeFS.writeFileSync(
      NodePath.join(repoRoot, ".env"),
      "T3CODE_RELAY_URL=https://official.example.test\nEXPO_PUBLIC_CLERK_GOOGLE_ANDROID_CLIENT_ID=official\n",
    );
    const env = loadRepoEnv({
      repoRoot,
      baseEnv: {
        T3CODE_SELF_HOSTED_ONLY: "1",
        T3CODE_CLERK_PUBLISHABLE_KEY: "pk_official",
        T3CODE_SELF_HOSTED_CONNECT_URL: "https://preview.example.test",
      },
    });
    expect(env).toMatchObject({
      VITE_T3CODE_SELF_HOSTED_CONNECT_URL: "https://preview.example.test",
      T3CODE_CLERK_PUBLISHABLE_KEY: "",
      VITE_CLERK_PUBLISHABLE_KEY: "",
      EXPO_PUBLIC_CLERK_PUBLISHABLE_KEY: "",
      EXPO_PUBLIC_CLERK_GOOGLE_ANDROID_CLIENT_ID: "",
      T3CODE_RELAY_URL: "",
      VITE_T3CODE_RELAY_URL: "",
    });
  });
});

function makeTemporaryDirectory() {
  const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3code-public-config-"));
  temporaryDirectories.push(directory);
  return directory;
}
