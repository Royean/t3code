import { T, useGT } from "gt-react";
import { RouteIcon } from "lucide-react";

import { AlwaysOnRoutingSettings } from "./AlwaysOnRoutingSettings";
import { useSettingsScope } from "./SettingsScopeContext";
import { SettingsPageContainer, SettingsSection } from "./settingsLayout";

export function AlwaysOnSettings() {
  const gt = useGT();
  const { connectedEnvironments, environment: selectedEnvironment } = useSettingsScope();
  const environment =
    connectedEnvironments.find(
      (entry) => entry.environmentId === selectedEnvironment?.environmentId,
    ) ?? connectedEnvironments[0];

  return (
    <SettingsPageContainer>
      <SettingsSection id="always-on-routing" title={gt("Event routing")} icon={<RouteIcon />}>
        {environment ? (
          <AlwaysOnRoutingSettings environment={environment} />
        ) : (
          <p className="text-muted-foreground text-sm">
            <T>Connect an environment first.</T>
          </p>
        )}
      </SettingsSection>
    </SettingsPageContainer>
  );
}
