import { createFileRoute } from "@tanstack/react-router";

import { AlwaysOnSettings } from "../components/settings/AlwaysOnSettings";

export const Route = createFileRoute("/_chat/settings/always-on")({
  component: AlwaysOnSettings,
});
