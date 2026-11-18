import { describe, expect, it, vi } from "vite-plus/test";

import { SelfHostedConnectClientError, type SelfHostedConnectClient } from "./selfHostedConnect.ts";
import {
  createSelfHostedSessionController,
  type SelfHostedStoredProfile,
} from "./selfHostedSession.ts";

const NOW = Date.parse("2026-11-18T00:00:00.000Z");
const session = (token = "first") => ({
  accountId: "owner",
  accessToken: `access-${token}`,
  refreshToken: `refresh-${token}`,
  expiresAt: "2026-11-18T01:00:00.000Z",
});
const environment = {
  environmentId: "air",
  label: "Mac Air",
  httpBaseUrl: "https://air.example.test",
  wsBaseUrl: "wss://air.example.test",
  available: true,
};

function deferred<T>() {
  return Promise.withResolvers<T>();
}

function setup(saved: SelfHostedStoredProfile | null = null) {
  let stored = saved;
  const storage = {
    load: vi.fn(async () => stored),
    save: vi.fn(async (profile: SelfHostedStoredProfile) => {
      stored = profile;
    }),
    clear: vi.fn(async () => {
      stored = null;
    }),
  };
  const api = {
    login: vi.fn(async () => session()),
    refresh: vi.fn(async () => session("rotated")),
    logout: vi.fn(async () => {}),
    listEnvironments: vi.fn<SelfHostedConnectClient["listEnvironments"]>(async () => ({
      environments: [environment],
    })),
    connect: vi.fn<SelfHostedConnectClient["connect"]>(async () => ({
      environmentId: "air",
      pairingUrl: "https://air.example.test/pair#token=once",
      expiresAt: "2026-11-18T00:01:00.000Z",
    })),
  } satisfies SelfHostedConnectClient;
  const pair = vi.fn(async () => {});
  const removeEnvironment = vi.fn(async () => {});
  const isRegistered = vi.fn(() => false);
  const createClient = vi.fn(() => api);
  const controller = createSelfHostedSessionController({
    storage,
    pair,
    removeEnvironment,
    isRegistered,
    createClient,
    now: () => NOW,
  });
  return {
    controller,
    storage,
    api,
    pair,
    removeEnvironment,
    isRegistered,
    createClient,
    stored: () => stored,
  };
}

const credentials = {
  serverUrl: "https://connect.example.test",
  username: "owner",
  password: "secret",
};
const profile = {
  serverUrl: credentials.serverUrl,
  username: "owner",
  refreshToken: "refresh-old",
  environmentIds: ["air"],
};

describe("self-hosted account session", () => {
  it("restores with a rotated refresh token, lists automatically, and forgets only this profile's computers", async () => {
    const h = setup(profile);
    expect(await h.controller.restore()).toBe(true);
    expect(h.api.refresh).toHaveBeenCalledWith("refresh-old");
    expect(h.api.listEnvironments).toHaveBeenCalledWith("access-rotated");
    expect(h.stored()).toEqual({ ...profile, refreshToken: "refresh-rotated" });
    expect(h.controller.getSnapshot()).toMatchObject({
      signedIn: true,
      busy: false,
      environments: [environment],
    });
    await h.controller.signOut();
    expect(h.api.logout).toHaveBeenCalledWith("refresh-rotated");
    expect(h.removeEnvironment).toHaveBeenCalledExactlyOnceWith("air");
    expect(h.stored()).toBeNull();
    expect(h.controller.getSnapshot()).toMatchObject({ signedIn: false, environments: [] });
  });

  it("refreshes expired access before listing and forgets a rejected refresh session", async () => {
    const h = setup();
    h.api.login.mockResolvedValueOnce({ ...session(), expiresAt: "2026-11-18T00:00:20.000Z" });
    await h.controller.login(credentials);
    expect(h.api.refresh).toHaveBeenCalledWith("refresh-first");
    expect(h.api.listEnvironments).toHaveBeenCalledWith("access-rotated");
    await h.controller.connect("air");
    h.api.listEnvironments.mockRejectedValueOnce(
      new SelfHostedConnectClientError("unauthorized", 401, "Expired"),
    );
    h.api.refresh.mockRejectedValueOnce(
      new SelfHostedConnectClientError("unauthorized", 401, "Revoked"),
    );
    expect(await h.controller.refreshEnvironments()).toBe(false);
    expect(h.controller.getSnapshot()).toMatchObject({
      signedIn: false,
      busy: false,
      environments: [],
    });
    expect(h.stored()).toBeNull();
    expect(h.removeEnvironment).toHaveBeenCalledWith("air");
  });

  it("ignores an old server's late login and revokes the abandoned session", async () => {
    const h = setup();
    const pending = deferred<ReturnType<typeof session>>();
    const started = deferred<void>();
    h.api.login.mockImplementationOnce(async () => {
      started.resolve();
      return pending.promise;
    });
    const first = h.controller.login(credentials);
    await started.promise;
    expect(
      await h.controller.login({
        ...credentials,
        serverUrl: "https://other.example.test",
        username: "second",
      }),
    ).toBe(true);
    pending.resolve(session("abandoned"));
    expect(await first).toBe(false);
    expect(h.controller.getSnapshot()).toMatchObject({
      serverUrl: "https://other.example.test",
      username: "second",
      signedIn: true,
      busy: false,
    });
    expect(h.stored()).toMatchObject({
      serverUrl: "https://other.example.test",
      refreshToken: "refresh-first",
    });
    expect(h.api.logout).toHaveBeenCalledWith("refresh-abandoned");
  });

  it("rejects unavailable, unlisted, mismatched and expired pairing targets before pairing", async () => {
    const h = setup();
    h.api.listEnvironments.mockResolvedValueOnce({
      environments: [environment, { ...environment, environmentId: "offline", available: false }],
    });
    await h.controller.login(credentials);
    expect(await h.controller.connect("unknown")).toBe(false);
    expect(await h.controller.connect("offline")).toBe(false);
    expect(h.api.connect).not.toHaveBeenCalled();
    h.api.connect.mockResolvedValueOnce({
      environmentId: "other",
      pairingUrl: "https://other.example.test/pair",
      expiresAt: "2026-11-18T00:01:00.000Z",
    });
    expect(await h.controller.connect("air")).toBe(false);
    h.api.connect.mockResolvedValueOnce({
      environmentId: "air",
      pairingUrl: "https://air.example.test/pair",
      expiresAt: "2026-11-17T00:00:00.000Z",
    });
    expect(await h.controller.connect("air")).toBe(false);
    expect(h.pair).not.toHaveBeenCalled();
    expect(await h.controller.connect("air")).toBe(true);
    expect(h.pair).toHaveBeenCalledWith("https://air.example.test/pair#token=once", "air");
    expect(h.stored()?.environmentIds).toEqual(["air"]);
  });

  it("removes a computer if pairing finishes after sign out without restoring the session", async () => {
    const h = setup();
    const paired = deferred<void>();
    const started = deferred<void>();
    h.pair.mockImplementationOnce(async () => {
      started.resolve();
      await paired.promise;
    });
    await h.controller.login(credentials);
    const connecting = h.controller.connect("air");
    await started.promise;
    const signingOut = h.controller.signOut();
    paired.resolve();
    await signingOut;
    expect(await connecting).toBe(false);
    expect(h.removeEnvironment).toHaveBeenCalledExactlyOnceWith("air");
    expect(h.stored()).toBeNull();
    expect(h.controller.getSnapshot()).toMatchObject({
      signedIn: false,
      busy: false,
      environments: [],
    });
  });

  it("keeps refresh credentials after an offline restore and retries on re-entry", async () => {
    const h = setup(profile);
    h.api.refresh.mockRejectedValueOnce(
      new SelfHostedConnectClientError("upstream_timeout", 0, "Offline"),
    );
    expect(await h.controller.restore()).toBe(false);
    expect(h.stored()).toEqual(profile);
    expect(await h.controller.restore()).toBe(true);
    expect(h.controller.getSnapshot().signedIn).toBe(true);
  });

  it("retains a remembered profile's computers through reauthentication and removes them on account change", async () => {
    const h = setup({ ...profile, refreshToken: null });
    expect(await h.controller.restore()).toBe(true);
    expect(h.api.refresh).not.toHaveBeenCalled();
    h.api.login.mockRejectedValueOnce(
      new SelfHostedConnectClientError("unauthorized", 401, "Invalid credentials"),
    );
    expect(await h.controller.login(credentials)).toBe(false);
    expect(h.removeEnvironment).not.toHaveBeenCalled();
    expect(h.stored()).toEqual({ ...profile, refreshToken: null });
    const restarted = setup(h.stored());
    expect(await restarted.controller.restore()).toBe(true);
    expect(await restarted.controller.login(credentials)).toBe(true);
    expect(restarted.stored()?.environmentIds).toEqual(["air"]);
    await restarted.controller.login({ ...credentials, serverUrl: "https://other.example.test" });
    expect(restarted.removeEnvironment).toHaveBeenCalledExactlyOnceWith("air");
    expect(restarted.stored()?.environmentIds).toEqual([]);
  });

  it("preserves connection ownership across restart if revocation fails during reauthentication", async () => {
    const h = setup(profile);
    h.api.refresh.mockRejectedValueOnce(
      new SelfHostedConnectClientError("upstream_timeout", 0, "Offline"),
    );
    expect(await h.controller.restore()).toBe(false);
    h.api.logout.mockRejectedValueOnce(new Error("Offline"));
    expect(await h.controller.login(credentials)).toBe(false);
    expect(h.stored()).toEqual({ ...profile, refreshToken: null });
    expect(h.removeEnvironment).not.toHaveBeenCalled();
    const restarted = setup(h.stored());
    expect(await restarted.controller.restore()).toBe(true);
    expect(await restarted.controller.login({ ...credentials, username: "another" })).toBe(true);
    expect(restarted.removeEnvironment).toHaveBeenCalledExactlyOnceWith("air");
  });

  it("keeps computers paired while a refresh request is pending so sign out removes them", async () => {
    const h = setup();
    const pending = deferred<ReturnType<typeof session>>();
    const started = deferred<void>();
    await h.controller.login(credentials);
    h.api.listEnvironments.mockRejectedValueOnce(
      new SelfHostedConnectClientError("unauthorized", 401, "Expired"),
    );
    h.api.refresh.mockImplementationOnce(async () => {
      started.resolve();
      return pending.promise;
    });
    const refreshing = h.controller.refreshEnvironments();
    await started.promise;
    expect(await h.controller.connect("air")).toBe(true);
    expect(h.stored()?.environmentIds).toEqual(["air"]);
    expect(h.controller.getSnapshot().busy).toBe(true);
    pending.resolve(session("rotated"));
    expect(await refreshing).toBe(true);
    expect(h.stored()?.environmentIds).toEqual(["air"]);
    await h.controller.signOut();
    expect(h.removeEnvironment).toHaveBeenCalledExactlyOnceWith("air");
  });

  it("reuses an already rotated token when another request returns a delayed unauthorized response", async () => {
    const h = setup();
    const delayedList = deferred<{ environments: (typeof environment)[] }>();
    const listStarted = deferred<void>();
    const connectStarted = deferred<void>();
    const releaseConnect = deferred<void>();
    const expired = () => new SelfHostedConnectClientError("unauthorized", 401, "Expired");
    let currentAccessToken = "access-first";
    await h.controller.login(credentials);
    h.api.listEnvironments.mockImplementationOnce(async () => {
      listStarted.resolve();
      return delayedList.promise;
    });
    h.api.refresh.mockImplementation(async () => {
      const next = session(`rotated-${h.api.refresh.mock.calls.length}`);
      currentAccessToken = next.accessToken;
      return next;
    });
    h.api.connect.mockImplementation(async (accessToken) => {
      if (accessToken === "access-first") throw expired();
      connectStarted.resolve();
      await releaseConnect.promise;
      if (accessToken !== currentAccessToken) throw expired();
      return {
        environmentId: "air",
        pairingUrl: "https://air.example.test/pair#token=once",
        expiresAt: "2026-11-18T00:01:00.000Z",
      };
    });
    const listing = h.controller.refreshEnvironments();
    await listStarted.promise;
    const connecting = h.controller.connect("air");
    await connectStarted.promise;
    delayedList.reject(expired());
    expect(await listing).toBe(true);
    expect(h.api.refresh).toHaveBeenCalledTimes(1);
    releaseConnect.resolve();
    expect(await connecting).toBe(true);
    expect(h.controller.getSnapshot().signedIn).toBe(true);
    expect(h.stored()?.refreshToken).toBe("refresh-rotated-1");
  });

  it("does not expire a newer session when an in-flight retry is rejected after rotation", async () => {
    const h = setup();
    const retryStarted = deferred<void>();
    const releaseRetry = deferred<void>();
    const expired = () => new SelfHostedConnectClientError("unauthorized", 401, "Expired");
    await h.controller.login(credentials);
    h.api.refresh.mockImplementation(async () =>
      session(`rotated-${h.api.refresh.mock.calls.length}`),
    );
    h.api.connect.mockRejectedValueOnce(expired()).mockImplementationOnce(async () => {
      retryStarted.resolve();
      await releaseRetry.promise;
      throw expired();
    });
    const connecting = h.controller.connect("air");
    await retryStarted.promise;
    expect(await h.controller.restore()).toBe(true);
    releaseRetry.resolve();
    expect(await connecting).toBe(false);
    expect(h.api.connect).toHaveBeenCalledTimes(2);
    expect(h.api.refresh).toHaveBeenCalledTimes(2);
    expect(h.controller.getSnapshot().signedIn).toBe(true);
    expect(h.stored()?.refreshToken).toBe("refresh-rotated-2");
    expect(h.api.logout).not.toHaveBeenCalled();
  });
});
