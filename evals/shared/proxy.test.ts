import { expect, it } from "@effect/vitest";
import { proxyConfig } from "./proxy.ts";

function configured(values: Record<string, string | undefined>, run: () => void) {
  const previous = Object.fromEntries(Object.keys(values).map((key) => [key, process.env[key]]));
  try {
    for (const [key, value] of Object.entries(values)) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
    run();
  } finally {
    for (const [key, value] of Object.entries(previous)) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
  }
}

it("uses an explicit eval OpenAI endpoint without requiring a Codex proxy provider", () => {
  configured(
    {
      T3_EVAL_OPENAI_API_KEY: "synthetic-eval-key",
      T3_EVAL_OPENAI_BASE_URL: "https://eval.invalid/v1///",
    },
    () => {
      expect(proxyConfig()).toEqual({
        url: "https://eval.invalid/v1",
        headers: { Authorization: "Bearer synthetic-eval-key" },
      });
    },
  );
});

it("defaults explicit eval credentials to the OpenAI endpoint", () => {
  configured(
    { T3_EVAL_OPENAI_API_KEY: "synthetic-eval-key", T3_EVAL_OPENAI_BASE_URL: undefined },
    () => {
      expect(proxyConfig().url).toBe("https://api.openai.com/v1");
    },
  );
});

it("rejects incomplete or credential-bearing eval configuration without exposing its values", () => {
  configured(
    { T3_EVAL_OPENAI_API_KEY: undefined, T3_EVAL_OPENAI_BASE_URL: "https://eval.invalid/v1" },
    () => {
      expect(() => proxyConfig()).toThrow(/^OpenAI eval credentials are not configured\.$/);
    },
  );
  for (const endpoint of [
    "not a URL",
    "https://user:synthetic-url-secret@eval.invalid/v1",
    "https://eval.invalid/v1?key=synthetic-secret",
    "file:///synthetic",
  ]) {
    configured(
      { T3_EVAL_OPENAI_API_KEY: "synthetic-eval-key", T3_EVAL_OPENAI_BASE_URL: endpoint },
      () => {
        expect(() => proxyConfig()).toThrow(/^OpenAI eval endpoint is invalid\.$/);
      },
    );
  }
});
