import { createSelfHostedSessionController } from "@t3tools/client-runtime/self-hosted-session";
import { createSelfHostedConnectClient } from "@t3tools/client-runtime/self-hosted-connect";
import { runAtomCommand, squashAtomCommandFailure } from "@t3tools/client-runtime/state/runtime";
import { EnvironmentId } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as SecureStore from "expo-secure-store";
import { fetch as expoFetch } from "expo/fetch";

import { environmentCatalog } from "../../connection/catalog";
import { connectPairingUrl } from "../../connection/onboarding";
import { whenProtectedDataAvailable } from "../../lib/protectedData";
import { appAtomRegistry } from "../../state/atom-registry";

const KEY = "t3code.self-hosted-connect.v1";
const DEVICE_ONLY = { keychainAccessible: SecureStore.WHEN_UNLOCKED_THIS_DEVICE_ONLY };
const StoredProfile = Schema.Struct({
  serverUrl: Schema.String,
  username: Schema.String,
  refreshToken: Schema.NullOr(Schema.String),
  environmentIds: Schema.Array(Schema.String),
});
const decodeProfile = Schema.decodeSync(Schema.fromJsonString(StoredProfile));

export const selfHostedSession = createSelfHostedSessionController({
  createClient: (serverUrl) => createSelfHostedConnectClient({ serverUrl, fetch: expoFetch }),
  storage: {
    load: async () => {
      await whenProtectedDataAvailable();
      const raw = await SecureStore.getItemAsync(KEY, DEVICE_ONLY);
      return raw ? decodeProfile(raw) : null;
    },
    save: (profile) => SecureStore.setItemAsync(KEY, JSON.stringify(profile), DEVICE_ONLY),
    clear: () => SecureStore.deleteItemAsync(KEY, DEVICE_ONLY),
  },
  isRegistered: (id) =>
    appAtomRegistry.get(environmentCatalog.catalogValueAtom).entries.has(EnvironmentId.make(id)),
  pair: async (pairingUrl, id) => {
    const result = await runAtomCommand(
      appAtomRegistry,
      connectPairingUrl,
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
