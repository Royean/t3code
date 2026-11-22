import { useUser } from "@clerk/react";
import { useGT } from "gt-react";
import { UserRoundIcon } from "lucide-react";
import { useSyncExternalStore } from "react";

import { hasCloudPublicConfig } from "../../cloud/publicConfig";
import { selfHostedSession } from "../../connection/selfHostedConnection";
import { SidebarMenu, SidebarMenuButton, SidebarMenuItem } from "../ui/sidebar";

interface SettingsAccountTriggerProps {
  readonly isActive: boolean;
  readonly onOpen: () => void;
}

function AccountButton({
  isActive,
  onOpen,
  name,
  imageUrl,
}: SettingsAccountTriggerProps & {
  readonly name: string | undefined;
  readonly imageUrl?: string | undefined;
}) {
  const gt = useGT();
  const label = gt("T3 Account");
  return (
    <SidebarMenu>
      <SidebarMenuItem>
        <SidebarMenuButton
          size="icon"
          aria-label={label}
          aria-current={isActive ? "page" : undefined}
          title={label}
          isActive={isActive}
          onClick={onOpen}
        >
          {imageUrl ? (
            <img src={imageUrl} alt="" className="size-7 rounded-full object-cover" />
          ) : name ? (
            <span className="grid size-7 place-items-center rounded-full bg-primary text-xs text-primary-foreground">
              {Array.from(name).slice(0, 2).join("").toUpperCase()}
            </span>
          ) : (
            <UserRoundIcon />
          )}
        </SidebarMenuButton>
      </SidebarMenuItem>
    </SidebarMenu>
  );
}

function CloudAccountButton(
  props: SettingsAccountTriggerProps & { readonly name: string | undefined },
) {
  const { user } = useUser();
  return <AccountButton {...props} imageUrl={user?.imageUrl} />;
}

export function SettingsAccountTrigger(props: SettingsAccountTriggerProps) {
  const account = useSyncExternalStore(selfHostedSession.subscribe, selfHostedSession.getSnapshot);
  const name = account.signedIn ? account.username : undefined;
  return hasCloudPublicConfig() ? (
    <CloudAccountButton {...props} name={name} />
  ) : (
    <AccountButton {...props} name={name} />
  );
}
