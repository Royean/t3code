import { createFileRoute } from "@tanstack/react-router";

import { T3AccountSettings } from "../components/settings/T3AccountSettings";

export const Route = createFileRoute("/_chat/settings/account")({
  component: T3AccountSettings,
});
