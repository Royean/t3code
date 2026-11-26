// Eval-only OpenAI Responses configuration; credentials stay in process memory.
// @effect-diagnostics nodeBuiltinImport:off
import * as NodeChildProcess from "node:child_process";
import * as Schema from "effect/Schema";

// Read credentials into memory only. Python's TOML parser avoids another runner dependency.
const decodeProxyConfig = Schema.decodeUnknownSync(
  Schema.Struct({ url: Schema.String, headers: Schema.Record(Schema.String, Schema.String) }),
);
/** Explicit eval credentials take precedence over the optional Codex proxy. */
export const proxyConfig = () => {
  const key = process.env.T3_EVAL_OPENAI_API_KEY;
  const endpoint = process.env.T3_EVAL_OPENAI_BASE_URL;
  if (key !== undefined || endpoint !== undefined) {
    if (!key?.trim() || /[\r\n]/.test(key))
      throw new Error("OpenAI eval credentials are not configured.");
    let url: URL;
    try {
      url = new URL(endpoint ?? "https://api.openai.com/v1");
    } catch {
      throw new Error("OpenAI eval endpoint is invalid.");
    }
    if (
      !["https:", "http:"].includes(url.protocol) ||
      url.username ||
      url.password ||
      url.search ||
      url.hash
    )
      throw new Error("OpenAI eval endpoint is invalid.");
    return {
      url: url.href.replace(/\/+$/, ""),
      headers: { Authorization: `Bearer ${key.trim()}` },
    };
  }
  return decodeProxyConfig(
    JSON.parse(
      NodeChildProcess.execFileSync(
        "python3",
        [
          "-c",
          `
import json, os, pathlib, tomllib
home = pathlib.Path.home() / '.codex'
p = tomllib.loads((home / 'config.toml').read_text())['model_providers']['vibeproxy']
h = p.get('http_headers', {}).copy()
h.update({k: os.environ[v] for k,v in p.get('env_http_headers', {}).items()})
key = os.environ[p['env_key']] if p.get('env_key') else p.get('experimental_bearer_token')
if not key and p.get('requires_openai_auth'): key = json.loads((home / 'auth.json').read_text()).get('OPENAI_API_KEY')
if key: h['Authorization'] = 'Bearer ' + key
print(json.dumps({'url': p['base_url'].rstrip('/'), 'headers': h}))
`,
        ],
        { encoding: "utf8" },
      ),
    ),
  );
};
