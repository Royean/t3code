// @effect-diagnostics-next-line nodeBuiltinImport:off -- This is the application-owned native HTTP runtime boundary.
import * as NodeHttp from "node:http";
import * as ManagedRuntime from "effect/ManagedRuntime";
import { loadConfig } from "./config.ts";
import { createHttpHandler } from "./http.ts";
import { hashPassword } from "./password.ts";
import * as SelfHostedConnect from "./SelfHostedConnect.ts";

async function main() {
  const args = process.argv.slice(2);
  if (args[0] === "hash-password" && args.length === 1) {
    if (process.stdin.isTTY) throw new Error("Read the password from redirected stdin.");
    const chunks: Buffer[] = [];
    let bytes = 0;
    for await (const chunk of process.stdin) {
      bytes += Buffer.byteLength(chunk);
      if (bytes > 1_026) throw new Error("Password input exceeds the size limit.");
      chunks.push(Buffer.from(chunk));
    }
    process.stdout.write(
      `${await hashPassword(
        Buffer.concat(chunks)
          .toString("utf8")
          .replace(/\r?\n$/, ""),
      )}\n`,
    );
    return;
  }
  if (args.length !== 2 || args[0] !== "--config") {
    throw new Error("Usage: node src/bin.ts --config /absolute/path/config.json | hash-password");
  }
  const config = await loadConfig(args[1]!);
  const runtime = ManagedRuntime.make(SelfHostedConnect.layer(config));
  try {
    const service = await runtime.runPromise(SelfHostedConnect.SelfHostedConnect);
    const handler = createHttpHandler(service, config);
    const server = NodeHttp.createServer((request, response) => {
      void handler(request, response);
    });
    server.requestTimeout = 15_000;
    server.headersTimeout = 10_000;
    server.keepAliveTimeout = 5_000;
    server.maxHeadersCount = 50;
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(config.port ?? 3_050, config.host ?? "127.0.0.1", resolve);
    });
    process.stdout.write("Self-hosted Connect is ready.\n");
    await new Promise<void>((resolve) => {
      const stop = () => {
        server.close(() => resolve());
        server.closeIdleConnections();
      };
      process.once("SIGINT", stop);
      process.once("SIGTERM", stop);
    });
  } finally {
    await runtime.dispose();
  }
}

void main().catch(() => {
  // Configuration and upstream failures can contain secrets; startup only reports a fixed message.
  process.stderr.write(
    "Self-hosted Connect could not start. Check its private configuration and runtime.\n",
  );
  process.exitCode = 1;
});
