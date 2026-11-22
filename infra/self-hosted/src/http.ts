// @effect-diagnostics-next-line nodeBuiltinImport:off -- These are the native HTTP adapter's callback types.
import type * as NodeHttp from "node:http";
import {
  SelfHostedAndroidRead,
  SelfHostedAndroidRegisterInput,
  SelfHostedLoginInput,
  SelfHostedRefreshInput,
  SelfHostedRevokeDeviceInput,
  type SelfHostedErrorResponse,
} from "@t3tools/contracts/self-hosted-connect";
import { RelayAgentActivityPublishRequest } from "@t3tools/contracts/relay";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type { SelfHostedConfig } from "./config.ts";
import * as SelfHostedConnect from "./SelfHostedConnect.ts";
import type * as WebhookQueue from "./WebhookQueue.ts";

// @effect-diagnostics-next-line extendsNativeError:off -- Parse exceptions remain inside the Promise HTTP adapter and never enter an Effect failure channel.
class RequestError extends Error {
  readonly status: number;
  constructor(status: number) {
    super("The request is invalid.");
    this.status = status;
  }
}
type Reply = { readonly status: number; readonly body: unknown; readonly retryAfter?: number };
type ErrorCode = SelfHostedErrorResponse["error"]["code"];
const errorReply = (status: number, code: ErrorCode, message: string): Reply => ({
  status,
  body: { error: { code, message } },
});

function readBody(request: NodeHttp.IncomingMessage, limit: number): Promise<Buffer> {
  if (Number(request.headers["content-length"] ?? 0) > limit) {
    request.resume();
    return Promise.reject(new RequestError(413));
  }
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    let length = 0;
    let failed = false;
    // @effect-diagnostics-next-line globalTimers:off -- The native HTTP stream needs a deadline before an Effect operation is selected.
    const timer = setTimeout(() => {
      failed = true;
      reject(new RequestError(408));
    }, 10_000);
    timer.unref();
    request.on("data", (chunk: Buffer) => {
      length += chunk.length;
      if (length > limit) {
        failed = true;
        clearTimeout(timer);
        reject(new RequestError(413));
      } else if (!failed) chunks.push(chunk);
    });
    request.on("end", () => {
      clearTimeout(timer);
      if (failed) return;
      resolve(Buffer.concat(chunks));
    });
    request.on("error", () => {
      clearTimeout(timer);
      reject(new RequestError(400));
    });
  });
}

async function readJson(request: NodeHttp.IncomingMessage): Promise<unknown> {
  if (!/^application\/json(?:\s*;|$)/i.test(request.headers["content-type"] ?? "")) {
    throw new RequestError(415);
  }
  const body = await readBody(request, 16_384);
  try {
    return JSON.parse(body.toString("utf8"));
  } catch {
    throw new RequestError(400);
  }
}

const strippedWebhookHeaders = new Set([
  "connection",
  "host",
  "content-length",
  "transfer-encoding",
  "upgrade",
  "expect",
  "keep-alive",
  "proxy-authenticate",
  "proxy-authorization",
  "te",
  "trailer",
  "authorization",
  "cookie",
  "forwarded",
  "traceparent",
  "tracestate",
]);

function decode<S extends Schema.Top & { readonly DecodingServices: never }>(
  schema: S,
  value: unknown,
): S["Type"] {
  try {
    return Schema.decodeUnknownSync(schema, { onExcessProperty: "error" })(value);
  } catch {
    throw new RequestError(400);
  }
}

const bearer = (request: NodeHttp.IncomingMessage) => {
  const match = /^Bearer ([A-Za-z0-9_-]{43})$/i.exec(request.headers.authorization ?? "");
  return match?.[1] ?? "";
};

export function createHttpHandler(
  service: SelfHostedConnect.SelfHostedConnect["Service"],
  config: SelfHostedConfig,
) {
  type ServiceFailure =
    | SelfHostedConnect.Unauthorized
    | SelfHostedConnect.RateLimited
    | SelfHostedConnect.EnvironmentNotFound
    | SelfHostedConnect.EnvironmentUnavailable
    | SelfHostedConnect.UpstreamTimeout
    | SelfHostedConnect.StorageError
    | SelfHostedConnect.PasswordError
    | WebhookQueue.WebhookQueueFull;
  const run = <A>(effect: Effect.Effect<A, ServiceFailure>, status = 200, signal?: AbortSignal) =>
    Effect.runPromise(
      effect.pipe(
        Effect.map((body): Reply => ({ status, body })),
        Effect.catchTags({
          Unauthorized: () =>
            Effect.succeed(
              errorReply(
                401,
                "unauthorized",
                "The credentials are invalid or the session has expired.",
              ),
            ),
          RateLimited: (error: SelfHostedConnect.RateLimited) =>
            Effect.succeed({
              ...errorReply(429, "rate_limited", error.message),
              retryAfter: error.retryAfterSeconds,
            }),
          EnvironmentNotFound: () =>
            Effect.succeed(
              errorReply(404, "environment_not_found", "The environment is not configured."),
            ),
          EnvironmentUnavailable: () =>
            Effect.succeed(
              errorReply(
                503,
                "environment_unavailable",
                "The environment is unavailable. Try again later.",
              ),
            ),
          UpstreamTimeout: () =>
            Effect.succeed(
              errorReply(504, "upstream_timeout", "The environment did not respond in time."),
            ),
          StorageError: () =>
            Effect.succeed(
              errorReply(500, "internal_error", "The connection service is unavailable."),
            ),
          PasswordError: () =>
            Effect.succeed(
              errorReply(500, "internal_error", "The connection service is unavailable."),
            ),
          WebhookQueueFull: () =>
            Effect.succeed({
              ...errorReply(429, "rate_limited", "The webhook queue is full. Try again later."),
              retryAfter: 30,
            }),
        }),
      ),
      signal ? { signal } : undefined,
    );
  const route = async (
    request: NodeHttp.IncomingMessage,
    response: NodeHttp.ServerResponse,
  ): Promise<Reply> => {
    const path = request.url ?? "";
    if (request.method === "GET" && path === "/health") return { status: 200, body: { ok: true } };
    if (request.method === "POST" && path === "/v1/auth/login") {
      const input = decode(SelfHostedLoginInput, await readJson(request));
      return run(service.login(input, request.socket.remoteAddress ?? "unknown"));
    }
    if (request.method === "POST" && path === "/v1/auth/refresh") {
      const input = decode(SelfHostedRefreshInput, await readJson(request));
      return run(service.refresh(input.refreshToken));
    }
    if (request.method === "POST" && path === "/v1/auth/logout") {
      const input = decode(SelfHostedRefreshInput, await readJson(request));
      return run(service.logout(input.refreshToken).pipe(Effect.as({ ok: true })));
    }
    if (request.method === "GET" && path === "/v1/environments") {
      return run(service.listEnvironments(bearer(request)));
    }
    if (request.method === "GET" && path === "/v1/devices") {
      return run(service.listDevices(bearer(request)));
    }
    if (request.method === "POST" && path === "/v1/devices/revoke") {
      const { id } = decode(SelfHostedRevokeDeviceInput, await readJson(request));
      return run(service.revokeDevice(bearer(request), id).pipe(Effect.as({ ok: true })));
    }
    if (request.method === "POST" && path === "/v1/mobile/devices") {
      const { deliveryTokenHash, registration } = decode(
        SelfHostedAndroidRegisterInput,
        await readJson(request),
      );
      return run(
        service
          .registerAndroid(bearer(request), registration, deliveryTokenHash)
          .pipe(Effect.as({ ok: true })),
      );
    }
    if (request.method === "POST" && path === "/v1/mobile/notifications") {
      const { after, replay } = decode(SelfHostedAndroidRead, await readJson(request));
      const controller = new AbortController();
      const abort = () => controller.abort();
      response.once("close", abort);
      try {
        return await run(
          service.readAndroid(bearer(request), after, replay),
          200,
          controller.signal,
        );
      } finally {
        response.off("close", abort);
      }
    }
    const hook =
      /^\/v1\/hooks\/([A-Za-z0-9][A-Za-z0-9._-]{0,127})\/([A-Za-z0-9._-]{1,128})\/([A-Za-z0-9_-]{32,128})\/([A-Za-z0-9_-]{43})(?:\?([^#]{0,8192}))?$/.exec(
        path,
      );
    if (hook && request.method && ["POST", "PUT", "PATCH", "GET"].includes(request.method)) {
      const headers = Object.fromEntries(
        Object.entries(request.headers).flatMap(([name, value]) =>
          typeof value === "string" &&
          !strippedWebhookHeaders.has(name) &&
          !name.startsWith("x-t3-") &&
          !name.startsWith("x-forwarded-")
            ? [[name, value]]
            : [],
        ),
      );
      return run(
        service.queueWebhook({
          environmentId: hook[1]!,
          hookId: hook[2]!,
          token: hook[3]!,
          signature: hook[4]!,
          query: hook[5] ?? "",
          method: request.method,
          headers,
          body: await readBody(request, 1_048_576),
        }),
        202,
      );
    }
    const publish =
      /^\/v1\/environments\/([A-Za-z0-9][A-Za-z0-9._-]{0,127})\/threads\/([^/?#]{1,512})\/agent-activity$/.exec(
        path,
      );
    if (request.method === "POST" && publish) {
      const input = decode(RelayAgentActivityPublishRequest, await readJson(request));
      let threadId: string;
      try {
        threadId = decodeURIComponent(publish[2]!);
      } catch {
        throw new RequestError(400);
      }
      return run(
        service
          .publishActivity(bearer(request), publish[1]!, threadId, input.state)
          .pipe(Effect.as({ ok: true, deliveries: [] })),
      );
    }
    const match =
      /^\/v1\/environments\/([A-Za-z0-9][A-Za-z0-9._-]{0,127})\/(connect|session)$/.exec(path);
    if (request.method === "POST" && match) {
      const body = await readJson(request);
      if (
        body === null ||
        typeof body !== "object" ||
        Array.isArray(body) ||
        Object.keys(body).length !== 0
      ) {
        throw new RequestError(400);
      }
      if (match[2] === "session") return run(service.connectAccount(bearer(request), match[1]!));
      if (config.allowLegacyPairing !== false)
        return run(service.connect(bearer(request), match[1]!));
    }
    return errorReply(404, "not_found", "The endpoint does not exist.");
  };
  return async (
    request: NodeHttp.IncomingMessage,
    response: NodeHttp.ServerResponse,
  ): Promise<void> => {
    response.setHeader("Cache-Control", "no-store");
    response.setHeader("Pragma", "no-cache");
    response.setHeader("X-Content-Type-Options", "nosniff");
    response.setHeader("Content-Type", "application/json; charset=utf-8");
    response.setHeader("Vary", "Origin");
    const origin = request.headers.origin;
    const allowed = origin === undefined || (config.allowedOrigins ?? []).includes(origin);
    if (origin && allowed) response.setHeader("Access-Control-Allow-Origin", origin);
    let reply: Reply;
    try {
      if (!allowed) reply = errorReply(403, "invalid_request", "This origin is not permitted.");
      else if (request.method === "OPTIONS") {
        response.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
        response.setHeader("Access-Control-Allow-Headers", "Authorization, Content-Type");
        reply = { status: 204, body: undefined };
      } else reply = await route(request, response);
    } catch (cause) {
      reply =
        cause instanceof RequestError
          ? errorReply(cause.status, "invalid_request", cause.message)
          : errorReply(500, "internal_error", "The connection service is unavailable.");
    }
    if (response.destroyed) return;
    if (reply.retryAfter !== undefined) response.setHeader("Retry-After", reply.retryAfter);
    if (reply.status === 413 || reply.status === 408) response.setHeader("Connection", "close");
    response.writeHead(reply.status);
    response.end(reply.body === undefined ? undefined : JSON.stringify(reply.body));
  };
}
