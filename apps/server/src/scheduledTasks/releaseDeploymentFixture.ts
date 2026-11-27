// @effect-diagnostics nodeBuiltinImport:off -- This scoped fixture owns an independent durable adapter, coordinator, and physical target.
import * as NodeHttp from "node:http";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeSqlite from "node:sqlite";
import * as NodeChildProcess from "node:child_process";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as Path from "effect/Path";
import * as Config from "../config.ts";
import {
  WorkbenchDeploymentError,
  type WorkbenchDeploymentPayload,
  type WorkbenchDeploymentSigned,
  type WorkbenchDeploymentOperation,
  type WorkbenchDeploymentPreflightInput,
  type WorkbenchDeploymentTarget,
} from "@t3tools/contracts";
import { releaseDigest, releaseHash, canonicalReleaseJson } from "./workbenchReleaseProfile.ts";

export const deploymentFixture = Effect.fn("DeploymentFixture.acquire")(function* (
  dir: string,
  baselineSha: string,
  existing?: { bytes: string; version: string },
) {
  const path = yield* Path.Path,
    cfg = yield* Config.ServerConfig;
  const keys = NodeCrypto.generateKeyPairSync("ed25519"),
    publicKey = keys.publicKey.export({ type: "spki", format: "pem" }).toString();
  const keyId = releaseHash(keys.publicKey.export({ type: "spki", format: "der" }));
  const store = yield* Effect.acquireRelease(
    Effect.sync(() => ({
      database: new NodeSqlite.DatabaseSync(path.join(dir, "independent-adapter.sqlite")),
    })),
    (store) => Effect.sync(() => store.database.close()),
  );
  let db = store.database;
  const artifacts = path.join(dir, "adapter-artifacts"),
    targetFile = path.join(dir, "actual-online.bin");
  NodeFS.mkdirSync(artifacts);
  NodeFS.writeFileSync(targetFile, existing?.bytes ?? "prior-artifact");
  db.exec(
    "CREATE TABLE journal(id TEXT PRIMARY KEY,kind TEXT NOT NULL,input TEXT NOT NULL,result TEXT NOT NULL); CREATE TABLE meta(id TEXT PRIMARY KEY,value TEXT NOT NULL); CREATE TABLE claims(id TEXT PRIMARY KEY,value TEXT NOT NULL)",
  );
  const meta = (id: string) =>
    JSON.parse(
      (db.prepare("SELECT value FROM meta WHERE id=?").get(id) as { value: string }).value,
    );
  const putMeta = (id: string, value: unknown) =>
    db
      .prepare("INSERT INTO meta VALUES(?,?) ON CONFLICT(id) DO UPDATE SET value=excluded.value")
      .run(id, JSON.stringify(value));
  const baseline = {
    serviceId: "service",
    version: existing?.version ?? "v1",
    sourceSha: baselineSha,
    artifactSha256: releaseHash(existing?.bytes ?? "prior-artifact"),
  };
  putMeta("target", { revision: 0, lastOperationId: null, state: "consistent", basis: baseline });
  putMeta("fence", { epoch: 0, nonce: null });
  putMeta("writes", 0);
  putMeta("operation_puts", 0);
  let origin = "",
    mode: "success" | "before" | "partial" | "rollback_failure" = "success",
    loseResponse = false,
    loseClaim = false,
    loseRollbackResponse = false,
    operationReads = true,
    adapterAvailable = true,
    requestCount = 0,
    healthFault = false,
    coreFault = false,
    checkCount = 0,
    versionFault: "http500" | "disconnect" | "invalid_json" | null = null,
    versionFaultOnlyCandidate = true;
  type Gate = {
    point:
      | "before_apply"
      | "after_apply"
      | "after_claim"
      | "health_check"
      | "core_check"
      | "version_read"
      | "target_read";
    arrive: () => void;
    release: () => void;
    complete: () => void;
    reached: Effect.Effect<void>;
    wait: Promise<void>;
    completed: Effect.Effect<void>;
  };
  const checkExecutions: string[] = [];
  let nextGate: Gate | undefined;
  const gates = new Set<Gate>();
  const gateNext = (point: Gate["point"]) => {
    let arrive!: () => void, release!: () => void, complete!: () => void;
    const reached = new Promise<void>((resolve) => {
        arrive = resolve;
      }),
      wait = new Promise<void>((resolve) => {
        release = resolve;
      }),
      completed = new Promise<void>((resolve) => {
        complete = resolve;
      });
    const gate = {
      point,
      arrive,
      release,
      complete,
      reached: Effect.promise(() => reached),
      wait,
      completed: Effect.promise(() => completed),
    };
    nextGate = gate;
    gates.add(gate);
    return gate;
  };
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      for (const gate of gates) {
        gate.release();
        gate.complete();
      }
    }),
  );
  const descriptor = {
    schemaVersion: 1 as const,
    adapterId: "adapter",
    adapterVersion: 1,
    generationId: "generation",
    serviceKey: "formal-service",
    serviceId: "service",
    coordinatorId: keyId,
    deploy: { actionId: "activate", planDigest: releaseHash("fixture-activate-v1") },
    rollback: { actionId: "restore", planDigest: releaseHash("fixture-restore-v1") },
  };
  const sign = (
    payload: WorkbenchDeploymentPayload,
    kind: "reservation" | "commit",
  ): WorkbenchDeploymentSigned => {
    const unsigned = {
      schemaVersion: 1 as const,
      kind,
      keyId,
      authorizationDigest: releaseDigest(payload),
      payload,
      committedAt: kind === "commit" ? payload.createdAt + 1 : null,
    };
    return {
      ...unsigned,
      signature: NodeCrypto.sign(
        null,
        Buffer.from(releaseDigest(unsigned), "utf8"),
        keys.privateKey,
      ).toString("base64url"),
    };
  };
  const target = (): WorkbenchDeploymentTarget => {
    const value = meta("target");
    return {
      schemaVersion: 1,
      adapterId: descriptor.adapterId,
      adapterVersion: descriptor.adapterVersion,
      generationId: descriptor.generationId,
      serviceKey: descriptor.serviceKey,
      serviceId: descriptor.serviceId,
      ...value,
      basis: value.basis
        ? { ...value.basis, artifactSha256: releaseHash(NodeFS.readFileSync(targetFile)) }
        : null,
      observedAt: DateTime.formatIso(DateTime.nowUnsafe()),
    };
  };
  const currentBasis = () => {
    const t = target();
    return (
      t.basis ?? {
        ...baseline,
        version: "partial",
        artifactSha256: releaseHash(NodeFS.readFileSync(targetFile)),
      }
    );
  };
  const lookup = (id: string) =>
    db.prepare("SELECT * FROM journal WHERE id=?").get(id) as
      | { kind: string; input: string; result: string }
      | undefined;
  const record = (id: string, kind: string, input: unknown, result: unknown) =>
    db
      .prepare(
        "INSERT INTO journal VALUES(?,?,?,?) ON CONFLICT(id) DO UPDATE SET result=excluded.result",
      )
      .run(id, kind, canonicalReleaseJson(input), JSON.stringify(result));
  const json = (r: NodeHttp.ServerResponse, status: number, value: unknown) => {
    r.writeHead(status, { "content-type": "application/json" });
    r.end(JSON.stringify(value));
  };
  const error = (r: NodeHttp.ServerResponse, status: number, reason: string) =>
    json(r, status, { _tag: "ReleaseAdapterError", reason });
  const body = async (r: NodeHttp.IncomingMessage) => {
    const parts: Buffer[] = [];
    for await (const chunk of r) parts.push(Buffer.from(chunk));
    return Buffer.concat(parts);
  };
  const server = NodeHttp.createServer(async (req, res) => {
    requestCount++;
    try {
      const url = new URL(req.url!, "http://fixture"),
        route = url.pathname;
      if (route === "/checks/health" || route === "/checks/core") {
        checkCount++;
        if (typeof req.headers["x-release-execution"] === "string")
          checkExecutions.push(req.headers["x-release-execution"]);
        const point = route.endsWith("health") ? "health_check" : "core_check";
        if (nextGate?.point === point) {
          const gate = nextGate;
          nextGate = undefined;
          gate.arrive();
          await gate.wait;
          gate.complete();
        }
        const bytes = NodeFS.readFileSync(targetFile),
          failed = route.endsWith("health") ? healthFault : coreFault;
        return json(res, failed || bytes.toString() !== "candidate" ? 503 : 200, {
          serviceId: "service",
          artifactSha256: releaseHash(bytes),
          value: bytes.toString().toUpperCase(),
        });
      }
      if (route === "/version") {
        const gate = nextGate?.point === "version_read" ? nextGate : undefined;
        if (gate) {
          nextGate = undefined;
          gate.arrive();
          await gate.wait;
          gate.complete();
        }
        if (
          versionFault &&
          (!versionFaultOnlyCandidate || currentBasis().artifactSha256 !== baseline.artifactSha256)
        ) {
          if (versionFault === "disconnect") return res.destroy();
          if (versionFault === "invalid_json") {
            res.writeHead(200, { "content-type": "application/json" });
            return res.end("not-json");
          }
          return error(res, 500, "unavailable");
        }
        return json(res, 200, currentBasis());
      }
      if (route === "/.well-known/workbench-release-coordinator")
        return json(res, 200, { schemaVersion: 1, configured: true, id: keyId, origin, publicKey });
      if (route.startsWith("/api/release-coordinator/v1/claims/")) {
        const id = decodeURIComponent(route.split("/").at(-1)!);
        const row = db.prepare("SELECT value FROM claims WHERE id=?").get(id) as
          | { value: string }
          | undefined;
        if (!row) return error(res, 404, "not_found");
        const c = JSON.parse(row.value);
        if (
          releaseHash((req.headers.authorization ?? "").replace(/^Bearer /, "")) !==
          c.reservation.payload.claimTokenDigest
        )
          return error(res, 403, "forbidden");
        if (req.method === "POST") {
          const input = JSON.parse((await body(req)).toString());
          if (
            c.state === "withdrawn" ||
            input.authorizationDigest !== c.reservation.authorizationDigest ||
            input.nativeDeploymentId !== c.reservation.payload.nativeDeploymentId
          )
            return error(res, 403, "forbidden");
          c.state = "committed";
          c.authorization = sign(c.reservation.payload, "commit");
          db.prepare("UPDATE claims SET value=? WHERE id=?").run(JSON.stringify(c), id);
          if (nextGate?.point === "after_claim") {
            const gate = nextGate;
            nextGate = undefined;
            gate.arrive();
            await gate.wait;
            gate.complete();
          }
          if (loseClaim) {
            loseClaim = false;
            return res.destroy();
          }
        }
        return json(res, 200, {
          schemaVersion: 1,
          deploymentId: id,
          authorizationDigest: c.reservation.authorizationDigest,
          state: c.state,
          authorization: c.authorization ?? null,
        });
      }
      if (!route.startsWith("/release/")) return error(res, 404, "not_found");
      if (!adapterAvailable) return error(res, 503, "unavailable");
      if (req.headers.authorization !== "Bearer fixture-adapter-private")
        return error(res, 403, "forbidden");
      const id = decodeURIComponent(route.split("/").at(-1)!);
      if (route === "/release/descriptor") return json(res, 200, descriptor);
      if (route === "/release/target") {
        if (nextGate?.point === "target_read") {
          const gate = nextGate;
          nextGate = undefined;
          gate.arrive();
          await gate.wait;
          gate.complete();
        }
        return json(res, 200, target());
      }
      if (route.startsWith("/release/artifacts/")) {
        if (!/^[a-f0-9]{64}$/.test(id)) return error(res, 400, "invalid_request");
        const file = path.join(artifacts, id);
        if (req.method === "PUT") {
          const bytes = await body(req);
          if (releaseHash(bytes) !== id) return error(res, 409, "conflict");
          if (!NodeFS.existsSync(file)) NodeFS.writeFileSync(file, bytes, { flag: "wx" });
        }
        if (!NodeFS.existsSync(file)) return error(res, 404, "not_found");
        const bytes = NodeFS.readFileSync(file);
        return json(res, 200, { sha256: releaseHash(bytes), bytes: bytes.length });
      }
      if (route.startsWith("/release/preflights/")) {
        const existing = lookup(id);
        if (req.method === "GET")
          return existing
            ? json(res, 200, JSON.parse(existing.result))
            : error(res, 404, "not_found");
        const input: WorkbenchDeploymentPreflightInput = JSON.parse((await body(req)).toString());
        if (existing)
          return existing.input === canonicalReleaseJson(input)
            ? json(res, 200, JSON.parse(existing.result))
            : error(res, 409, "conflict");
        const { inputDigest, ...unsigned } = input;
        if (
          inputDigest !== releaseDigest(unsigned) ||
          input.descriptorDigest !== releaseDigest(descriptor)
        )
          return error(res, 409, "conflict");
        const stamp = DateTime.formatIso(DateTime.nowUnsafe());
        const result = NodeChildProcess.spawnSync(
          process.execPath,
          [
            "-e",
            "const f=require('node:fs'),c=require('node:crypto');for(const [p,h] of [[process.argv[1],process.argv[2]],[process.argv[3],process.argv[4]]])if(c.createHash('sha256').update(f.readFileSync(p)).digest('hex')!==h)process.exit(7);f.copyFileSync(process.argv[3],process.argv[5]);",
            path.join(artifacts, input.artifact.sha256),
            input.artifact.sha256,
            path.join(artifacts, input.baseline.artifact.sha256),
            input.baseline.artifact.sha256,
            path.join(dir, "nonproduction-restore.bin"),
          ],
          { env: {}, encoding: "utf8" },
        );
        const receipt = {
          ...descriptor,
          id,
          inputDigest,
          revision: 1,
          state: result.status === 0 ? "succeeded" : "failed",
          result: {
            exitCode: result.status ?? -1,
            outputDigest: releaseHash(result.stdout + result.stderr),
            completedAt: DateTime.formatIso(DateTime.nowUnsafe()),
          },
          createdAt: stamp,
          updatedAt: DateTime.formatIso(DateTime.nowUnsafe()),
        };
        const { serviceId: _sid, coordinatorId: _cid, deploy: _d, rollback: _r, ...safe } = receipt;
        record(id, "preflight", input, safe);
        return json(res, 200, safe);
      }
      if (route.startsWith("/release/operations/")) {
        if (req.method === "PUT") putMeta("operation_puts", meta("operation_puts") + 1);
        if (req.method === "GET" && !operationReads) return error(res, 503, "unavailable");
        const existing = lookup(id);
        if (req.method === "GET")
          return existing
            ? json(res, 200, JSON.parse(existing.result))
            : error(res, 404, "not_found");
        const input = JSON.parse((await body(req)).toString()),
          a: WorkbenchDeploymentSigned = input.authorization;
        if (existing)
          return existing.input === canonicalReleaseJson(input)
            ? json(res, 200, JSON.parse(existing.result))
            : error(res, 409, "conflict");
        const { signature, ...unsigned } = a;
        if (
          a.kind !== "commit" ||
          a.keyId !== keyId ||
          a.authorizationDigest !== releaseDigest(a.payload) ||
          !NodeCrypto.verify(
            null,
            Buffer.from(releaseDigest(unsigned), "utf8"),
            keys.publicKey,
            Buffer.from(signature, "base64url"),
          )
        )
          return error(res, 403, "forbidden");
        const p = a.payload,
          kind = input.kind as "deploy" | "rollback",
          expectedId = kind === "deploy" ? p.deployOperationId : p.rollbackOperationId;
        if (
          id !== expectedId ||
          input.inputDigest !==
            releaseDigest({ authorizationDigest: a.authorizationDigest, kind, operationId: id }) ||
          p.plan.generationId !== descriptor.generationId ||
          p.plan.serviceKey !== descriptor.serviceKey ||
          p.plan.deploy.planDigest !== descriptor.deploy.planDigest ||
          p.plan.rollback.planDigest !== descriptor.rollback.planDigest
        )
          return error(res, 409, "conflict");
        const fence = meta("fence");
        if (
          p.lock.epoch < fence.epoch ||
          (p.lock.epoch === fence.epoch && p.lock.nonce !== fence.nonce)
        )
          return error(res, 409, "conflict");
        const before = target();
        if (
          kind === "deploy" &&
          (before.revision !== p.baseline.revision ||
            canonicalReleaseJson(before.basis) !== canonicalReleaseJson(p.baseline.basis))
        )
          return error(res, 409, "conflict");
        if (kind === "rollback") {
          const parent = lookup(p.deployOperationId),
            op = parent ? (JSON.parse(parent.result) as WorkbenchDeploymentOperation) : null;
          if (
            !op?.result ||
            !["succeeded", "failed"].includes(op.state) ||
            before.lastOperationId !== op.id ||
            before.revision !== op.result.writtenRevision
          )
            return error(res, 409, "conflict");
        }
        putMeta("fence", p.lock);
        const createdAt = DateTime.formatIso(DateTime.nowUnsafe()),
          externalExecutionId = `external:${id}`;
        const base = {
          schemaVersion: 1 as const,
          adapterId: "adapter",
          adapterVersion: 1,
          generationId: "generation",
          serviceKey: "formal-service",
          serviceId: "service",
          id,
          kind,
          inputDigest: input.inputDigest,
          authorizationDigest: a.authorizationDigest,
          externalExecutionId,
          revision: 1,
          state: "running" as const,
          result: null,
          createdAt,
          updatedAt: createdAt,
        };
        record(id, kind, input, base);
        const gate =
          nextGate?.point === "before_apply" || nextGate?.point === "after_apply"
            ? nextGate
            : undefined;
        if (gate) nextGate = undefined;
        if (gate?.point === "before_apply") {
          gate.arrive();
          await gate.wait;
        }
        const shouldFail = kind === "deploy" ? mode === "before" : mode === "rollback_failure";
        let writtenRevision: number | null = null,
          effect: "unchanged" | "changed" | "partial" = "unchanged";
        if (
          !shouldFail &&
          !(
            kind === "deploy" &&
            canonicalReleaseJson(p.desired) === canonicalReleaseJson(before.basis)
          )
        ) {
          const artifact = kind === "deploy" ? p.artifact : p.rollbackArtifact,
            bytes = NodeFS.readFileSync(path.join(artifacts, artifact.sha256));
          if (releaseHash(bytes) !== artifact.sha256) return error(res, 409, "conflict");
          const partial = kind === "deploy" && (mode === "partial" || mode === "rollback_failure");
          NodeFS.writeFileSync(targetFile, partial ? bytes.subarray(0, 2) : bytes);
          writtenRevision = before.revision + 1;
          effect = partial ? "partial" : "changed";
          putMeta("target", {
            revision: writtenRevision,
            lastOperationId: id,
            state: partial ? "partial" : "consistent",
            basis: partial ? null : kind === "deploy" ? p.desired : p.baseline.basis,
          });
          putMeta("writes", meta("writes") + 1);
        }
        if (gate?.point === "after_apply") {
          gate.arrive();
          await gate.wait;
        }
        const completedAt = DateTime.formatIso(DateTime.nowUnsafe());
        const operation: WorkbenchDeploymentOperation = {
          ...base,
          revision: 2,
          state: effect === "partial" || shouldFail ? "failed" : "succeeded",
          result: {
            effect,
            writtenRevision,
            exitCode: effect === "partial" || shouldFail ? 7 : 0,
            outputDigest: releaseHash(NodeFS.readFileSync(targetFile)),
            completedAt,
          },
          updatedAt: completedAt,
        };
        record(id, kind, input, operation);
        gate?.complete();
        if (loseResponse || (kind === "rollback" && loseRollbackResponse)) {
          loseResponse = false;
          loseRollbackResponse = false;
          operationReads = false;
          return res.destroy();
        }
        return json(res, 200, operation);
      }
      return error(res, 404, "not_found");
    } catch {
      if (!res.destroyed) error(res, 503, "unavailable");
    }
  });
  yield* Effect.acquireRelease(
    Effect.tryPromise({
      try: () => new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve)),
      catch: () => new WorkbenchDeploymentError({ reason: "unavailable" }),
    }),
    () =>
      Effect.promise(
        () =>
          new Promise<void>((resolve) => {
            server.closeAllConnections();
            server.close(() => resolve());
          }),
      ),
  );
  const address = server.address();
  if (!address || typeof address === "string")
    return yield* new WorkbenchDeploymentError({ reason: "unavailable" });
  origin = `http://127.0.0.1:${address.port}`;
  NodeFS.writeFileSync(
    path.join(cfg.stateDir, "workbench-release-adapter-credentials.json"),
    canonicalReleaseJson({
      schemaVersion: 1,
      credentials: { "adapter-token": "fixture-adapter-private" },
    }),
  );
  const policy = {
    adapter: {
      id: "adapter",
      version: 1,
      generationId: "generation",
      origin,
      pathPrefix: "/release",
      credentialRef: "adapter-token",
    },
    serviceKey: "formal-service",
    coordinator: { id: keyId, origin, publicKey },
    deploy: descriptor.deploy,
    rollback: descriptor.rollback,
  };
  yield* Effect.logInfo(
    `issue16-native independent fixture ${dir}; port ${address.port}; cleanup scoped`,
  );
  return {
    policy,
    baseline,
    target,
    sign,
    gateNext,
    reserve: (payload: WorkbenchDeploymentPayload) => {
      const reservation = sign(payload, "reservation");
      db.prepare("INSERT INTO claims VALUES(?,?)").run(
        payload.deploymentId,
        JSON.stringify({ state: "reserved", reservation }),
      );
      return reservation;
    },
    withdraw: (id: string) => {
      const row = db.prepare("SELECT value FROM claims WHERE id=?").get(id) as { value: string };
      const c = JSON.parse(row.value);
      c.state = "withdrawn";
      db.prepare("UPDATE claims SET value=? WHERE id=?").run(JSON.stringify(c), id);
    },
    setMode: (next: typeof mode) => {
      mode = next;
    },
    loseNextResponse: () => {
      loseResponse = true;
    },
    loseNextRollbackResponse: () => {
      loseRollbackResponse = true;
    },
    loseNextClaimResponse: () => {
      loseClaim = true;
    },
    reopenLedger: () => {
      db.close();
      db = new NodeSqlite.DatabaseSync(path.join(dir, "independent-adapter.sqlite"));
      store.database = db;
    },
    checkCalls: () => checkCount,
    checkExecutions: () => [...checkExecutions],
    setCheckFault: (role: "health" | "core", value: boolean) => {
      if (role === "health") healthFault = value;
      else coreFault = value;
    },
    requests: () => requestCount,
    setVersionFault: (fault: typeof versionFault, onlyCandidate = true) => {
      versionFault = fault;
      versionFaultOnlyCandidate = onlyCandidate;
    },
    setAdapterAvailable: (value: boolean) => {
      adapterAvailable = value;
    },
    setOperationReads: (value: boolean) => {
      operationReads = value;
    },
    rotateGeneration: () => {
      descriptor.generationId = "rotated-generation";
    },
    operationPutRequests: () => meta("operation_puts") as number,
    corruptTargetBytes: () => NodeFS.writeFileSync(targetFile, "unrelated-bytes"),
    replaceTarget: () => {
      NodeFS.writeFileSync(targetFile, "third-party");
      const before = meta("target");
      putMeta("target", {
        revision: before.revision + 1,
        lastOperationId: "third-party-operation",
        state: "consistent",
        basis: { ...baseline, version: "third-party" },
      });
    },
    writes: () => meta("writes") as number,
    bytes: () => NodeFS.readFileSync(targetFile).toString(),
  };
});
