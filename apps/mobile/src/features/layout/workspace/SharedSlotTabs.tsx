import { msg, useMessages } from "gt-react-native";

import { SegmentedControl } from "../../../components/ui/SegmentedControl";
import type { SharedSlotTab } from "./navigation-tab-order";
import { useSelectWorkspaceTab } from "./useSelectWorkspaceTab";

const OPTIONS = [
  { value: "pullRequests", label: msg("Pull requests") },
  { value: "issues", label: msg("Issues", { $context: "screen title, list of issues" }) },
] as const;

export function SharedSlotTabs({ tab }: { readonly tab: SharedSlotTab }) {
  const messages = useMessages();
  const selectTab = useSelectWorkspaceTab();
  return (
    <SegmentedControl
      options={OPTIONS.map((option) => ({ ...option, label: messages(option.label) }))}
      selected={tab}
      onSelect={(next) => {
        if (next !== tab) selectTab(next);
      }}
      role="tab"
      className="w-full max-w-64"
    />
  );
}
