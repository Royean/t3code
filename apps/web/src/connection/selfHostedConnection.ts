import {
  createSelfHostedSessionController,
  type SelfHostedStoredProfile,
} from "@t3tools/client-runtime/self-hosted-session";
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

import { appAtomRegistry } from "../rpc/atomRegistry";
import { environmentCatalog } from "./catalog";
import { connectPairing } from "./onboarding";

const KEY = "t3code.self-hosted-connect.profile.v1";
const PublicProfile = Schema.Struct({
  serverUrl: Schema.String,
  username: Schema.String,
  environmentIds: Schema.Array(Schema.String),
  excludedEnvironmentIds: Schema.optionalKey(Schema.Array(Schema.String)),
});
const decodeProfile = Schema.decodeSync(Schema.fromJsonString(PublicProfile));
let memoryProfile: SelfHostedStoredProfile | null = null;

export const selfHostedOnly = import.meta.env.VITE_T3CODE_SELF_HOSTED_ONLY === "1";

export const selfHostedSession = createSelfHostedSessionController({
  autoConnect: selfHostedOnly,
  deviceLabel: () => (window.desktopBridge ? "t4code" : "Web browser"),
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
      const { refreshToken: _refreshToken, ...publicProfile } = profile;
      localStorage.setItem(KEY, JSON.stringify(publicProfile));
      memoryProfile = profile;
    },
    clear: async () => {
      memoryProfile = null;
      localStorage.removeItem(KEY);
    },
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
            connectPairing,
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
      connectPairing,
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
