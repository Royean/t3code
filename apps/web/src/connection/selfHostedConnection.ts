import {
  createSelfHostedSessionController,
  type SelfHostedStoredProfile,
} from "@t3tools/client-runtime/self-hosted-session";
import { runAtomCommand, squashAtomCommandFailure } from "@t3tools/client-runtime/state/runtime";
import { EnvironmentId } from "@t3tools/contracts";
import * as Schema from "effect/Schema";

import { appAtomRegistry } from "../rpc/atomRegistry";
import { environmentCatalog } from "./catalog";
import { connectPairing } from "./onboarding";

const KEY = "t3code.self-hosted-connect.profile.v1";
const PublicProfile = Schema.Struct({
  serverUrl: Schema.String,
  username: Schema.String,
  environmentIds: Schema.Array(Schema.String),
});
const decodeProfile = Schema.decodeSync(Schema.fromJsonString(PublicProfile));
let memoryProfile: SelfHostedStoredProfile | null = null;

export const selfHostedSession = createSelfHostedSessionController({
  storage: {
    load: async () => {
      if (memoryProfile) return memoryProfile;
      const raw = localStorage.getItem(KEY);
      return raw
        ? {
            ...decodeProfile(raw),
            refreshToken: null,
          }
        : null;
    },
    save: async (profile) => {
      // Account credentials stay in memory; only connection ownership survives a page reload.
      const { serverUrl, username, environmentIds } = profile;
      localStorage.setItem(KEY, JSON.stringify({ serverUrl, username, environmentIds }));
      memoryProfile = profile;
    },
    clear: async () => {
      memoryProfile = null;
      localStorage.removeItem(KEY);
    },
  },
  isRegistered: (id) =>
    appAtomRegistry.get(environmentCatalog.catalogValueAtom).entries.has(EnvironmentId.make(id)),
  pair: async (pairingUrl, id) => {
    const result = await runAtomCommand(
      appAtomRegistry,
      connectPairing,
      {
        pairingUrl,
        expectedEnvironmentId: EnvironmentId.make(id),
      },
      { reportFailure: false },
    );
    if (result._tag === "Failure") throw squashAtomCommandFailure(result);
  },
  removeEnvironment: async (id) => {
    const result = await runAtomCommand(
      appAtomRegistry,
      environmentCatalog.remove,
      EnvironmentId.make(id),
      { reportFailure: false },
    );
    if (result._tag === "Failure") throw squashAtomCommandFailure(result);
  },
});
