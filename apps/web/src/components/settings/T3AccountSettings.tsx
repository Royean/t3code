import { useAuth, useClerk } from "@clerk/react";
import { T, useGT } from "gt-react";

import { hasCloudPublicConfig } from "../../cloud/publicConfig";
import { retryClerkLoad, useClerkLoadFailed } from "../clerk/clerkLoadFailure";
import { useT3ConnectAccountPage } from "../clerk/T3ConnectAccountPages";
import { useT3ConnectAuthPrompt } from "../clerk/useT3ConnectAuthPrompt";
import { Button } from "../ui/button";
import { SelfHostedConnectSettings } from "./SelfHostedConnectSettings";
import { SettingsPageContainer, SettingsRow, SettingsSection } from "./settingsLayout";

function OfficialAccountRow() {
  const gt = useGT();
  const { isLoaded } = useAuth();
  const clerk = useClerk();
  const loadFailed = useClerkLoadFailed();
  const { openAuthPrompt } = useT3ConnectAuthPrompt();
  const { open, portals } = useT3ConnectAccountPage();

  return (
    <SettingsRow
      title={gt("Official account")}
      description={open ? gt("Signed in") : undefined}
      control={
        loadFailed ? (
          <Button size="sm" variant="outline" onClick={retryClerkLoad}>
            <T>Retry T3 Connect</T>
          </Button>
        ) : open ? (
          <div className="flex flex-wrap gap-2">
            <Button size="sm" variant="outline" onClick={() => open()}>
              <T>Manage account</T>
            </Button>
            <Button size="sm" variant="ghost" onClick={() => void clerk.signOut()}>
              <T>Sign out</T>
            </Button>
          </div>
        ) : (
          <Button size="sm" variant="outline" disabled={!isLoaded} onClick={openAuthPrompt}>
            {isLoaded ? gt("Sign in") : gt("Loading…")}
          </Button>
        )
      }
    >
      {portals}
    </SettingsRow>
  );
}

export function T3AccountSettings() {
  return (
    <SettingsPageContainer>
      {hasCloudPublicConfig() ? (
        <SettingsSection id="t3-connect-account" title="T3 Connect">
          <OfficialAccountRow />
        </SettingsSection>
      ) : null}
      <SelfHostedConnectSettings />
    </SettingsPageContainer>
  );
}
