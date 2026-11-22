import { createSelfHostedSessionController } from "@t3tools/client-runtime/self-hosted-session";
import { createSelfHostedConnectClient } from "@t3tools/client-runtime/self-hosted-connect";
import { runAtomCommand, squashAtomCommandFailure } from "@t3tools/client-runtime/state/runtime";
import {
  connectionRoutes,
  connectionRouteId,
  hasRouteToOrigin,
  selfHostedConnectionId,
} from "@t3tools/client-runtime/connection";
import { EnvironmentId } from "@t3tools/contracts";
import type { SelfHostedEnvironmentSession } from "@t3tools/contracts/self-hosted-connect";
import * as Schema from "effect/Schema";
import * as SecureStore from "expo-secure-store";
import Constants from "expo-constants";
import { fetch as expoFetch } from "expo/fetch";
import { Platform } from "react-native";

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
  excludedEnvironmentIds: Schema.optionalKey(Schema.Array(Schema.String)),
});
const decodeProfile = Schema.decodeSync(Schema.fromJsonString(StoredProfile));

export const selfHostedOnly = Constants.expoConfig?.extra?.selfHostedOnly === true;

export const selfHostedSession = createSelfHostedSessionController({
  autoConnect: selfHostedOnly,
  createClient: (serverUrl) => createSelfHostedConnectClient({ serverUrl, fetch: expoFetch }),
  deviceLabel: () =>
    (Constants.deviceName?.trim() || (Platform.OS === "android" ? "Android" : "iOS")).slice(0, 160),
  storage: {
    load: async () => {
      await whenProtectedDataAvailable();
      const raw = await SecureStore.getItemAsync(KEY, DEVICE_ONLY);
      return raw ? decodeProfile(raw) : null;
    },
    save: (profile) => SecureStore.setItemAsync(KEY, JSON.stringify(profile), DEVICE_ONLY),
    clear: () => SecureStore.deleteItemAsync(KEY, DEVICE_ONLY),
  },
  isRegistered: (id, origin, owned) => {
    const entry = appAtomRegistry
      .get(environmentCatalog.catalogValueAtom)
      .entries.get(EnvironmentId.make(id));
    return (
      entry !== undefined &&
      (entry.target._tag === "PrimaryConnectionTarget" ||
        hasRouteToOrigin(
          entry,
          origin,
          selfHostedOnly && owned ? selfHostedConnectionId(id, true) : undefined,
        ))
    );
  },
  ...(selfHostedOnly
    ? {
        registerAccountSession: async (accountSession: SelfHostedEnvironmentSession) => {
          const result = await runAtomCommand(
            appAtomRegistry,
            connectPairingUrl,
            {
              accountSession,
              expectedEnvironmentId: EnvironmentId.make(accountSession.environmentId),
            },
            { reportFailure: false },
          );
          if (result._tag === "Failure") throw squashAtomCommandFailure(result);
        },
      }
    : {}),
  pair: async (pairingUrl, id) => {
    const result = await runAtomCommand(
      appAtomRegistry,
      connectPairingUrl,
      {
        pairingUrl,
        expectedEnvironmentId: EnvironmentId.make(id),
        selfHosted: true,
      },
      { reportFailure: false },
    );
    if (result._tag === "Failure") throw squashAtomCommandFailure(result);
  },
  removeEnvironment: async (id) => {
    const environmentId = EnvironmentId.make(id);
    const entry = appAtomRegistry
      .get(environmentCatalog.catalogValueAtom)
      .entries.get(environmentId);
    if (!entry) return;
    const accountRoutes = new Set([
      selfHostedConnectionId(environmentId),
      selfHostedConnectionId(environmentId, true),
    ]);
    for (const { target } of connectionRoutes(entry)) {
      const routeId = connectionRouteId(target);
      if (!accountRoutes.has(routeId)) continue;
      const result = await runAtomCommand(
        appAtomRegistry,
        environmentCatalog.removeRoute,
        { environmentId, routeId },
        { reportFailure: false },
      );
      if (result._tag === "Failure") throw squashAtomCommandFailure(result);
    }
  },
});
