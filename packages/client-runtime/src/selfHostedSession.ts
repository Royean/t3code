import type {
  SelfHostedEnvironment,
  SelfHostedSession,
} from "@t3tools/contracts/self-hosted-connect";
import { msg } from "gt-i18n";

import { translatableError } from "./errors/translatable.ts";
import {
  createSelfHostedConnectClient,
  normalizeSelfHostedServerUrl,
  SelfHostedConnectClientError,
  type SelfHostedConnectClient,
} from "./selfHostedConnect.ts";

export interface SelfHostedStoredProfile {
  readonly serverUrl: string;
  readonly username: string;
  readonly refreshToken: string | null;
  readonly environmentIds: ReadonlyArray<string>;
}

export interface SelfHostedSessionStorage {
  readonly load: () => Promise<SelfHostedStoredProfile | null>;
  readonly save: (profile: SelfHostedStoredProfile) => Promise<void>;
  readonly clear: () => Promise<void>;
}

export interface SelfHostedSessionState {
  readonly serverUrl: string;
  readonly username: string;
  readonly signedIn: boolean;
  readonly busy: boolean;
  readonly environments: ReadonlyArray<SelfHostedEnvironment>;
  readonly error: unknown;
}

const EMPTY_STATE: SelfHostedSessionState = {
  serverUrl: "",
  username: "",
  signedIn: false,
  busy: false,
  environments: [],
  error: null,
};
const superseded = Symbol("self-hosted-session-superseded");
const unauthorized = (error: unknown) =>
  error instanceof SelfHostedConnectClientError && error.status === 401;

export function createSelfHostedSessionController(options: {
  readonly storage: SelfHostedSessionStorage;
  readonly pair: (pairingUrl: string, environmentId: string) => Promise<void>;
  readonly isRegistered: (environmentId: string) => boolean;
  readonly removeEnvironment: (environmentId: string) => Promise<void>;
  readonly createClient?: (serverUrl: string) => SelfHostedConnectClient;
  readonly now?: () => number;
}) {
  let state = EMPTY_STATE;
  let generation = 0;
  let restored = false;
  let profile: SelfHostedStoredProfile | null = null;
  let session: SelfHostedSession | null = null;
  let client: SelfHostedConnectClient | null = null;
  let restoring: Promise<boolean> | null = null;
  let refreshing: Promise<SelfHostedSession> | null = null;
  let writes = Promise.resolve();
  let cleanup = Promise.resolve();
  let pairing = Promise.resolve();
  let running = 0;
  const listeners = new Set<() => void>();
  const now = options.now ?? Date.now;
  const makeClient =
    options.createClient ?? ((serverUrl) => createSelfHostedConnectClient({ serverUrl }));
  const update = (patch: Partial<SelfHostedSessionState>) => {
    state = { ...state, ...patch };
    for (const listener of listeners) listener();
  };
  const current = (version: number) => {
    if (version !== generation) throw superseded;
  };
  const write = (operation: () => Promise<void>) => {
    const result = writes.then(operation);
    writes = result.catch(() => {});
    return result;
  };
  const persist = (version: number) => {
    const saved = profile;
    return write(async () => {
      if (version === generation && saved) await options.storage.save(saved);
    });
  };
  const revoke = async (api: SelfHostedConnectClient | null, refreshToken?: string | null) => {
    if (api && refreshToken) await api.logout(refreshToken);
  };
  const forget = async (keepEnvironments = false) => {
    const previous = profile;
    const previousClient = client;
    const previousSession = session;
    const retainedProfile =
      keepEnvironments && previous ? { ...previous, refreshToken: null } : null;
    const version = ++generation;
    profile = retainedProfile;
    session = null;
    client = null;
    refreshing = null;
    running = 0;
    restored = true;
    update({
      ...EMPTY_STATE,
      serverUrl: retainedProfile?.serverUrl ?? "",
      username: retainedProfile?.username ?? "",
      busy: true,
    });
    const previousPairing = pairing;
    const pending = cleanup
      .catch(() => {})
      .then(async () => {
        await previousPairing.catch(() => {});
        const results = await Promise.allSettled([
          write(() =>
            retainedProfile ? options.storage.save(retainedProfile) : options.storage.clear(),
          ),
          ...(keepEnvironments ? [] : (previous?.environmentIds ?? [])).map((id) =>
            options.removeEnvironment(id),
          ),
          revoke(previousClient, previousSession?.refreshToken ?? previous?.refreshToken),
        ]);
        const failure = results.find((result) => result.status === "rejected");
        if (failure?.status === "rejected") throw failure.reason;
      });
    cleanup = pending;
    try {
      await pending;
    } catch (error) {
      current(version);
      throw error;
    }
    current(version);
    return version;
  };
  const refresh = (version: number): Promise<SelfHostedSession> => {
    current(version);
    if (refreshing) return refreshing;
    const saved = profile;
    const api = client;
    if (!saved?.refreshToken || !api)
      throw translatableError(msg("Sign in to your connection server."));
    const refreshToken = saved.refreshToken;
    const pending = (async () => {
      const next = await api.refresh(refreshToken);
      if (version !== generation) {
        await revoke(api, next.refreshToken).catch(() => {});
        throw superseded;
      }
      session = next;
      if (!profile) throw superseded;
      profile = { ...profile, refreshToken: next.refreshToken };
      await persist(version);
      current(version);
      update({ signedIn: true });
      return next;
    })();
    refreshing = pending;
    void pending
      .finally(() => {
        if (refreshing === pending) refreshing = null;
      })
      .catch(() => {});
    return pending;
  };
  const authorized = async <T>(
    version: number,
    action: (token: string) => Promise<T>,
  ): Promise<T> => {
    current(version);
    const active =
      session && Date.parse(session.expiresAt) > now() + 30_000 ? session : await refresh(version);
    current(version);
    try {
      return await action(active.accessToken);
    } catch (error) {
      current(version);
      if (!unauthorized(error)) throw error;
      const next =
        session && session.accessToken !== active.accessToken ? session : await refresh(version);
      current(version);
      try {
        return await action(next.accessToken);
      } catch (retryError) {
        current(version);
        if (unauthorized(retryError) && session?.accessToken !== next.accessToken) throw superseded;
        throw retryError;
      }
    }
  };
  const list = async (version: number) => {
    const api = client;
    if (!api) throw superseded;
    const result = await authorized(version, api.listEnvironments);
    current(version);
    update({ environments: result.environments });
  };
  const run = async (version: number, action: () => Promise<void>) => {
    if (version !== generation) return false;
    running += 1;
    update({ busy: true, error: null });
    try {
      await action();
      current(version);
      return true;
    } catch (error) {
      if (version !== generation || error === superseded) return false;
      if (unauthorized(error) && profile?.refreshToken) {
        const expiredVersion = generation + 1;
        try {
          await forget();
        } catch {
          /* Local removal is still attempted if revocation fails. */
        }
        if (expiredVersion !== generation) return false;
        update({
          busy: false,
          error: translatableError(msg("Your session expired. Sign in again.")),
        });
      } else {
        update({ error });
      }
      return false;
    } finally {
      if (version === generation) {
        running -= 1;
        update({ busy: running > 0 });
      }
    }
  };

  return {
    getSnapshot: () => state,
    subscribe: (listener: () => void) => {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    restore: () => {
      if (restoring) return restoring;
      if (restored && !session) return Promise.resolve(true);
      const version = generation;
      const pending = run(version, async () => {
        if (restored) {
          await refresh(version);
          await list(version);
          return;
        }
        const saved = await options.storage.load();
        current(version);
        if (!saved) {
          restored = true;
          return;
        }
        profile = { ...saved, serverUrl: normalizeSelfHostedServerUrl(saved.serverUrl) };
        client = makeClient(profile.serverUrl);
        update({ serverUrl: profile.serverUrl, username: profile.username });
        if (!profile.refreshToken) {
          restored = true;
          return;
        }
        await refresh(version);
        await list(version);
        restored = true;
      });
      restoring = pending;
      void pending.finally(() => {
        if (restoring === pending) restoring = null;
      });
      return pending;
    },
    login: async (input: {
      readonly serverUrl: string;
      readonly username: string;
      readonly password: string;
    }) => {
      let serverUrl: string;
      try {
        serverUrl = normalizeSelfHostedServerUrl(input.serverUrl);
      } catch (error) {
        update({ error });
        return false;
      }
      let version: number;
      const retainedIds =
        profile?.serverUrl === serverUrl && profile.username === input.username.trim()
          ? profile.environmentIds
          : [];
      try {
        version = await forget(retainedIds.length > 0);
      } catch (error) {
        if (error !== superseded) update({ busy: false, error });
        return false;
      }
      return run(version, async () => {
        const api = makeClient(serverUrl);
        client = api;
        update({ serverUrl, username: input.username.trim() });
        const next = await api.login(input.username.trim(), input.password);
        if (version !== generation) {
          await revoke(api, next.refreshToken).catch(() => {});
          throw superseded;
        }
        session = next;
        profile = {
          serverUrl,
          username: input.username.trim(),
          refreshToken: next.refreshToken,
          environmentIds: retainedIds,
        };
        await persist(version);
        current(version);
        update({ signedIn: true });
        await list(version);
      });
    },
    refreshEnvironments: () => run(generation, () => list(generation)),
    connect: (environmentId: string) => {
      const version = generation;
      return run(version, async () => {
        const environment = state.environments.find(
          (entry) => entry.environmentId === environmentId,
        );
        if (!environment?.available)
          throw translatableError(msg("Choose an available environment."));
        if (options.isRegistered(environmentId)) return;
        const api = client;
        if (!api) throw translatableError(msg("Sign in to your connection server."));
        const connection = await authorized(version, (token) => api.connect(token, environmentId));
        current(version);
        if (connection.environmentId !== environmentId) {
          throw translatableError(msg("The connection server returned a different environment."));
        }
        if (!(Date.parse(connection.expiresAt) > now())) {
          throw translatableError(msg("The pairing link expired. Try connecting again."));
        }
        const pending = pairing
          .catch(() => {})
          .then(async () => {
            current(version);
            await options.pair(connection.pairingUrl, environmentId);
            if (version !== generation) {
              await options.removeEnvironment(environmentId);
              throw superseded;
            }
            if (profile)
              profile = {
                ...profile,
                environmentIds: [...new Set([...profile.environmentIds, environmentId])],
              };
            await persist(version);
          });
        pairing = pending;
        await pending;
      });
    },
    signOut: async () => {
      try {
        const version = await forget();
        current(version);
        update({ busy: false });
        return true;
      } catch (error) {
        if (error !== superseded) update({ busy: false, error });
        return false;
      }
    },
  };
}
