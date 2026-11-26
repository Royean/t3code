import { expect, it } from "@effect/vitest";
import { Tool } from "effect/ai";
import { WorkbenchToolkit } from "./tools.ts";

it("exposes only persisted business selectors, never execution authority", () => {
  const schema = Tool.getJsonSchema(WorkbenchToolkit.tools.t3_workbench_dispatch) as {
    properties: Record<string, unknown>;
    required: string[];
  };
  expect(Object.keys(schema.properties)).toEqual(["taskId", "businessDeliveryId"]);
  expect(schema.required).toEqual(["taskId"]);
});
