import { useAtomValue } from "@effect/atom-react";
import { Link } from "@tanstack/react-router";
import { errorDisplayMessage } from "@t3tools/client-runtime/errors";
import { EnvironmentId } from "@t3tools/contracts";
import { hasRouteToOrigin } from "@t3tools/client-runtime/connection";
import { T, useGT, useMessages } from "gt-react";
import { useEffect, useState, useSyncExternalStore } from "react";

import { environmentCatalog } from "../../connection/catalog";
import { selfHostedOnly, selfHostedSession } from "../../connection/selfHostedConnection";
import { ensureLocalApi } from "../../localApi";
import { Button } from "../ui/button";
import { Input } from "../ui/input";
import { SettingsRow, SettingsSection } from "./settingsLayout";

const configuredServerUrl = (
  import.meta.env.VITE_T3CODE_SELF_HOSTED_CONNECT_URL as string | undefined
)?.trim();

function useSelfHostedAccount() {
  const state = useSyncExternalStore(selfHostedSession.subscribe, selfHostedSession.getSnapshot);
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- reads remembered connection ownership when entering settings; credentials remain in memory
  useEffect(() => {
    void selfHostedSession.restore();
  }, []);
  return state;
}

export function SelfHostedConnectSettings() {
  const gt = useGT();
  const messages = useMessages();
  const state = useSelfHostedAccount();
  const [serverUrl, setServerUrl] = useState<string | null>(null);
  const [username, setUsername] = useState<string | null>(null);
  const [password, setPassword] = useState("");
  const address = configuredServerUrl || (serverUrl ?? state.serverUrl);
  const account = username ?? state.username;
  const canSubmit = !state.busy && Boolean(address.trim() && account.trim() && password);
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- refreshes the authenticated device list when the account screen opens
  useEffect(() => {
    if (state.signedIn) void selfHostedSession.refreshDevices();
  }, [state.signedIn]);

  return (
    <>
      <SettingsSection
        id="self-hosted-connect"
        title={selfHostedOnly ? "T3 Connect" : gt("Self-hosted connection")}
      >
        <div className="space-y-4 px-3 py-4 sm:px-4">
          {state.signedIn ? (
            <>
              <p className="break-all text-sm">{state.username}</p>
              <p className="break-all text-xs text-muted-foreground">{state.serverUrl}</p>
              <div className="flex flex-wrap gap-2">
                <Button
                  size="sm"
                  variant="outline"
                  render={<Link to="/settings/connections" hash="self-hosted-environments" />}
                >
                  <T>Import environments</T>
                </Button>
                <Button
                  size="sm"
                  variant="ghost"
                  disabled={state.busy}
                  onClick={() => {
                    setServerUrl(null);
                    setUsername(null);
                    setPassword("");
                    void selfHostedSession.signOut();
                  }}
                >
                  <T>Sign out</T>
                </Button>
              </div>
              <p className="text-xs text-muted-foreground">
                <T>Signing out removes this account's saved environments from this device.</T>
              </p>
            </>
          ) : (
            <form
              className="space-y-3"
              onSubmit={(event) => {
                event.preventDefault();
                if (!canSubmit) return;
                const attempt = { serverUrl: address, username: account, password };
                setPassword("");
                void selfHostedSession.login(attempt);
              }}
            >
              {configuredServerUrl ? (
                <p className="break-all text-xs text-muted-foreground">{configuredServerUrl}</p>
              ) : (
                <label className="block space-y-1.5">
                  <span className="text-xs font-medium">
                    <T>Server URL</T>
                  </span>
                  <Input
                    type="url"
                    value={address}
                    onChange={(event) => setServerUrl(event.target.value)}
                    placeholder="https://connect.example.com"
                    disabled={state.busy}
                    spellCheck={false}
                    autoCapitalize="none"
                    autoComplete="url"
                  />
                </label>
              )}
              <label className="block space-y-1.5">
                <span className="text-xs font-medium">
                  <T>Username</T>
                </span>
                <Input
                  value={account}
                  onChange={(event) => setUsername(event.target.value)}
                  disabled={state.busy}
                  spellCheck={false}
                  autoCapitalize="none"
                  autoComplete="username"
                />
              </label>
              <label className="block space-y-1.5">
                <span className="text-xs font-medium">
                  <T>Password</T>
                </span>
                <Input
                  type="password"
                  value={password}
                  onChange={(event) => setPassword(event.target.value)}
                  disabled={state.busy}
                  autoComplete="current-password"
                />
              </label>
              <Button type="submit" size="sm" variant="outline" disabled={!canSubmit}>
                {state.busy ? gt("Signing in…") : gt("Sign in")}
              </Button>
            </form>
          )}
          <p className="text-xs text-muted-foreground">
            <T>Reloading this page requires signing in again.</T>
          </p>
          {state.error ? (
            <p role="alert" className="text-xs text-destructive">
              {errorDisplayMessage(state.error, messages) ?? gt("Connection failed.")}
            </p>
          ) : null}
        </div>
      </SettingsSection>
      {state.devices ? (
        <SettingsSection id="self-hosted-devices" title={gt("Devices")}>
          {state.devices.map((device) => {
            const label = ["T3 Pro", "T4 code"].includes(device.label ?? "")
              ? "t4code"
              : (device.label ?? gt("Device"));
            return (
              <SettingsRow
                key={device.id}
                title={label}
                description={device.current ? gt("This device") : undefined}
                control={
                  device.current ? undefined : (
                    <Button
                      size="sm"
                      variant="outline"
                      disabled={state.busy}
                      onClick={async () => {
                        if (
                          await ensureLocalApi().dialogs.confirm(
                            gt("Sign out of {device}?", { device: label }),
                          )
                        ) {
                          await selfHostedSession.revokeDevice(device.id);
                        }
                      }}
                    >
                      <T>Sign out</T>
                    </Button>
                  )
                }
              />
            );
          })}
        </SettingsSection>
      ) : null}
    </>
  );
}

export function SelfHostedEnvironmentImports() {
  const gt = useGT();
  const messages = useMessages();
  const state = useSelfHostedAccount();
  const catalog = useAtomValue(environmentCatalog.catalogValueAtom);

  if (!state.signedIn) return null;

  return (
    <SettingsSection
      id="self-hosted-environments"
      title={gt("Self-hosted environments")}
      headerAction={
        <Button
          size="sm"
          variant="outline"
          disabled={state.busy}
          onClick={() => void selfHostedSession.refreshEnvironments()}
        >
          <T>Refresh</T>
        </Button>
      }
    >
      {state.environments.length === 0 ? (
        <p className="px-3 py-4 text-sm text-muted-foreground sm:px-4">
          <T>No registered computers.</T>
        </p>
      ) : null}
      {state.environments.map((environment) => {
        const entry = catalog.entries.get(EnvironmentId.make(environment.environmentId));
        const saved = entry !== undefined && hasRouteToOrigin(entry, environment.httpBaseUrl);
        return (
          <SettingsRow
            key={environment.environmentId}
            title={environment.label}
            description={environment.httpBaseUrl}
            control={
              <Button
                size="sm"
                variant="outline"
                disabled={state.busy || !catalog.isReady || saved || !environment.available}
                onClick={() => void selfHostedSession.connect(environment.environmentId)}
              >
                {!environment.available ? gt("Offline") : saved ? gt("Saved") : gt("Connect")}
              </Button>
            }
          />
        );
      })}
      {state.error ? (
        <p role="alert" className="px-3 py-4 text-xs text-destructive sm:px-4">
          {errorDisplayMessage(state.error, messages) ?? gt("Connection failed.")}
        </p>
      ) : null}
    </SettingsSection>
  );
}
