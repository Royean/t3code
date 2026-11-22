import {
  SelfHostedConnection,
  SelfHostedDevices,
  SelfHostedEnvironmentSession,
  SelfHostedEnvironments,
  SelfHostedErrorResponse,
  SelfHostedOk,
  SelfHostedSession,
} from "@t3tools/contracts/self-hosted-connect";
import * as Schema from "effect/Schema";
import type { RelayDeviceRegistrationRequest } from "@t3tools/contracts/relay";
const decodeErrorResponse = Schema.decodeUnknownSync(SelfHostedErrorResponse);

export type {
  SelfHostedConnection,
  SelfHostedDevice,
  SelfHostedEnvironment,
  SelfHostedEnvironments,
  SelfHostedSession,
} from "@t3tools/contracts/self-hosted-connect";

// @effect-diagnostics-next-line extendsNativeError:off -- This shared client exposes a Promise API rather than an Effect failure channel.
export class SelfHostedConnectClientError extends Error {
  override readonly name = "SelfHostedConnectClientError";
  readonly code: string;
  readonly status: number;
  constructor(code: string, status: number, message: string) {
    super(message);
    this.code = code;
    this.status = status;
  }
}

export function normalizeSelfHostedServerUrl(value: string): string {
  const url = new URL(value.trim());
  const loopback = ["localhost", "127.0.0.1", "[::1]"].includes(url.hostname);
  if (
    (url.protocol !== "https:" && !(url.protocol === "http:" && loopback)) ||
    url.username ||
    url.password ||
    url.pathname !== "/" ||
    url.search ||
    url.hash
  ) {
    throw new SelfHostedConnectClientError(
      "invalid_request",
      0,
      "Enter an HTTPS server origin, such as https://connect.example.com.",
    );
  }
  return url.origin;
}

export function createSelfHostedConnectClient(options: {
  readonly serverUrl: string;
  readonly fetch?: typeof globalThis.fetch;
}) {
  const origin = normalizeSelfHostedServerUrl(options.serverUrl);
  const fetchRequest = options.fetch ?? globalThis.fetch;
  const request = async <S extends Schema.Top & { readonly DecodingServices: never }>(
    path: string,
    schema: S,
    init: { readonly body?: unknown; readonly accessToken?: string } = {},
  ): Promise<S["Type"]> => {
    const controller = new AbortController();
    // @effect-diagnostics-next-line globalTimers:off -- The browser/mobile Promise boundary must abort its fetch even when the peer stalls.
    const timer = setTimeout(() => controller.abort(), 15_000);
    try {
      const response = await fetchRequest(new URL(path, origin), {
        method: init.body === undefined ? "GET" : "POST",
        headers: {
          ...(init.body === undefined ? {} : { "Content-Type": "application/json" }),
          ...(init.accessToken === undefined
            ? {}
            : { Authorization: `Bearer ${init.accessToken}` }),
        },
        ...(init.body === undefined ? {} : { body: JSON.stringify(init.body) }),
        credentials: "omit",
        cache: "no-store",
        redirect: "error",
        signal: controller.signal,
      });
      const payload: unknown = await response.json();
      if (!response.ok) {
        const error = decodeErrorResponse(payload);
        throw new SelfHostedConnectClientError(
          error.error.code,
          response.status,
          error.error.message,
        );
      }
      return Schema.decodeUnknownSync(schema)(payload);
    } catch (cause) {
      if (cause instanceof SelfHostedConnectClientError) throw cause;
      throw new SelfHostedConnectClientError(
        controller.signal.aborted ? "upstream_timeout" : "invalid_response",
        0,
        controller.signal.aborted
          ? "The connection server did not respond in time."
          : "The connection server could not be reached or returned an invalid response.",
      );
    } finally {
      clearTimeout(timer);
    }
  };
  return {
    login: (username: string, password: string, deviceLabel?: string) =>
      request("/v1/auth/login", SelfHostedSession, {
        body: { username, password, deviceLabel },
      }),
    refresh: (refreshToken: string) =>
      request("/v1/auth/refresh", SelfHostedSession, { body: { refreshToken } }),
    logout: async (refreshToken: string): Promise<void> => {
      await request("/v1/auth/logout", SelfHostedOk, {
        body: { refreshToken },
      });
    },
    listDevices: (accessToken: string) =>
      request("/v1/devices", SelfHostedDevices, { accessToken }),
    revokeDevice: (accessToken: string, id: string) =>
      request("/v1/devices/revoke", SelfHostedOk, { accessToken, body: { id } }),
    registerAndroidDevice: (
      accessToken: string,
      body: RelayDeviceRegistrationRequest,
      deliveryTokenHash: string,
    ) =>
      request("/v1/mobile/devices", SelfHostedOk, {
        accessToken,
        body: { registration: body, deliveryTokenHash },
      }),
    listEnvironments: (accessToken: string) =>
      request("/v1/environments", SelfHostedEnvironments, { accessToken }),
    connectAccount: (accessToken: string, environmentId: string) =>
      request(
        `/v1/environments/${encodeURIComponent(environmentId)}/session`,
        SelfHostedEnvironmentSession,
        {
          accessToken,
          body: {},
        },
      ),
    connect: (accessToken: string, environmentId: string) =>
      request(
        `/v1/environments/${encodeURIComponent(environmentId)}/connect`,
        SelfHostedConnection,
        {
          accessToken,
          body: {},
        },
      ),
  };
}

export type SelfHostedConnectClient = ReturnType<typeof createSelfHostedConnectClient>;
