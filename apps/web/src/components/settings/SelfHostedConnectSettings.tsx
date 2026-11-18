import { useAtomValue } from "@effect/atom-react";
import { errorDisplayMessage } from "@t3tools/client-runtime/errors";
import { EnvironmentId } from "@t3tools/contracts";
import { T, useGT, useMessages } from "gt-react";
import { useEffect, useState, useSyncExternalStore } from "react";

import { environmentCatalog } from "../../connection/catalog";
import { selfHostedSession } from "../../connection/selfHostedConnection";
import { Button } from "../ui/button";
import { Input } from "../ui/input";
import { FoldedSettingsSection } from "./FoldedSettingsSection";

export function SelfHostedConnectSettings() {
  const gt = useGT();
  const messages = useMessages();
  const state = useSyncExternalStore(selfHostedSession.subscribe, selfHostedSession.getSnapshot);
  const catalog = useAtomValue(environmentCatalog.catalogValueAtom);
  const [serverUrl, setServerUrl] = useState<string | null>(null);
  const [username, setUsername] = useState<string | null>(null);
  const [password, setPassword] = useState("");
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- reads remembered connection ownership when entering settings; credentials remain in memory
  useEffect(() => {
    void selfHostedSession.restore();
  }, []);
  const address = serverUrl ?? state.serverUrl;
  const account = username ?? state.username;

  return (
    <FoldedSettingsSection
      id="self-hosted-connect"
      title={gt("Self-hosted connection")}
      summary={state.signedIn ? state.username : null}
    >
      <div className="space-y-4 px-3 py-4 sm:px-4">
        {state.signedIn ? (
          <>
            <p className="break-all text-xs text-muted-foreground">
              {state.username} · {state.serverUrl}
            </p>
            {state.environments.length === 0 ? (
              <p className="text-sm text-muted-foreground">
                <T>No registered computers.</T>
              </p>
            ) : null}
            {state.environments.map((environment) => {
              const saved = catalog.entries.has(EnvironmentId.make(environment.environmentId));
              return (
                <div
                  key={environment.environmentId}
                  className="flex items-center justify-between gap-3"
                >
                  <div className="min-w-0">
                    <p className="truncate text-sm">{environment.label}</p>
                    <p className="truncate text-xs text-muted-foreground">
                      {environment.httpBaseUrl}
                    </p>
                  </div>
                  <Button
                    size="sm"
                    variant="outline"
                    disabled={state.busy || !catalog.isReady || saved || !environment.available}
                    onClick={() => {
                      void selfHostedSession.connect(environment.environmentId);
                    }}
                  >
                    {saved
                      ? gt("Connected")
                      : environment.available
                        ? gt("Connect")
                        : gt("Offline")}
                  </Button>
                </div>
              );
            })}
            <div className="flex flex-wrap gap-2">
              <Button
                size="sm"
                variant="outline"
                disabled={state.busy}
                onClick={() => {
                  void selfHostedSession.refreshEnvironments();
                }}
              >
                <T>Refresh</T>
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
              if (state.busy || !address.trim() || !account.trim() || !password) return;
              const attempt = { serverUrl: address, username: account, password };
              setPassword("");
              void selfHostedSession.login(attempt);
            }}
          >
            <p className="text-xs text-muted-foreground">
              <T>Sign in to your own connection server.</T>
            </p>
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
            <Button
              type="submit"
              size="sm"
              variant="outline"
              disabled={state.busy || !address.trim() || !account.trim() || !password}
            >
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
    </FoldedSettingsSection>
  );
}
