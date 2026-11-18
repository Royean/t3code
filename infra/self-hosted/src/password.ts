// @effect-diagnostics-next-line nodeBuiltinImport:off -- Node's scrypt and constant-time comparison implement the stored password format.
import * as NodeCrypto from "node:crypto";

const PARAMETERS = { N: 32_768, r: 8, p: 1, maxmem: 64 * 1_024 * 1_024 };
const HASH_PATTERN = /^scrypt\$32768\$8\$1\$([a-f0-9]{32})\$([a-f0-9]{64})$/;

const derive = (password: string, salt: Buffer): Promise<Buffer> =>
  new Promise((resolve, reject) => {
    NodeCrypto.scrypt(password, salt, 32, PARAMETERS, (error, key) =>
      error ? reject(error) : resolve(key),
    );
  });

export function isPasswordHash(value: string): boolean {
  return HASH_PATTERN.test(value);
}

export async function hashPassword(password: string): Promise<string> {
  if (password.length < 12 || Buffer.byteLength(password) > 1_024) {
    throw new Error("The password must contain at least 12 characters and at most 1024 bytes.");
  }
  const salt = NodeCrypto.randomBytes(16);
  const key = await derive(password, salt);
  return `scrypt$32768$8$1$${salt.toString("hex")}$${key.toString("hex")}`;
}

export async function verifyPassword(password: string, encoded: string): Promise<boolean> {
  const match = HASH_PATTERN.exec(encoded);
  if (!match || Buffer.byteLength(password) > 1_024) return false;
  const expected = Buffer.from(match[2]!, "hex");
  return NodeCrypto.timingSafeEqual(
    await derive(password, Buffer.from(match[1]!, "hex")),
    expected,
  );
}
