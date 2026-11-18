import { useAtomValue } from "@effect/atom-react";
import { errorDisplayMessage } from "@t3tools/client-runtime/errors";
import { EnvironmentId } from "@t3tools/contracts";
import { T, useGT, useMessages } from "gt-react-native";
import { useEffect, useState, useSyncExternalStore } from "react";
import { View } from "react-native";

import { AppText as Text, AppTextInput } from "../../components/ui/AppText";
import { Button } from "../../components/ui/Button";
import { ListRow } from "../../components/ui/ListRow";
import { environmentCatalog } from "../../connection/catalog";
import { selfHostedSession } from "./selfHostedConnection";

export function SelfHostedConnectSection({ onConnected }: { readonly onConnected?: () => void }) {
  const gt = useGT();
  const messages = useMessages();
  const state = useSyncExternalStore(selfHostedSession.subscribe, selfHostedSession.getSnapshot);
  const catalog = useAtomValue(environmentCatalog.catalogValueAtom);
  const [serverUrl, setServerUrl] = useState<string | null>(null);
  const [username, setUsername] = useState<string | null>(null);
  const [password, setPassword] = useState("");
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- restores encrypted account credentials when entering connection settings
  useEffect(() => {
    void selfHostedSession.restore();
  }, []);

  const address = serverUrl ?? state.serverUrl;
  const account = username ?? state.username;
  const signIn = () => {
    const attempt = { serverUrl: address, username: account, password };
    setPassword("");
    void selfHostedSession.login(attempt);
  };

  return (
    <View collapsable={false} className="mt-5 gap-2">
      <Text className="px-4 text-sm font-t3-medium text-foreground-muted">
        <T>Self-hosted connection</T>
      </Text>
      <View
        collapsable={false}
        className="gap-4 overflow-hidden rounded-[26px] bg-grouped-card p-4"
      >
        {state.signedIn ? (
          <>
            <Text className="text-sm text-foreground-muted">
              {state.username} · {state.serverUrl}
            </Text>
            {state.environments.length === 0 ? (
              <Text>
                <T>No registered computers.</T>
              </Text>
            ) : null}
            {state.environments.map((environment) => {
              const saved = catalog.entries.has(EnvironmentId.make(environment.environmentId));
              return (
                <ListRow
                  key={environment.environmentId}
                  label={environment.label}
                  description={environment.httpBaseUrl}
                  detail={
                    saved ? gt("Connected") : environment.available ? gt("Connect") : gt("Offline")
                  }
                  disabled={state.busy || !catalog.isReady || saved || !environment.available}
                  onPress={() => {
                    void selfHostedSession.connect(environment.environmentId).then((connected) => {
                      if (connected) onConnected?.();
                    });
                  }}
                />
              );
            })}
            <View className="flex-row flex-wrap gap-3">
              <Button
                label={gt("Refresh")}
                size="compact"
                disabled={state.busy}
                onPress={() => {
                  void selfHostedSession.refreshEnvironments();
                }}
              />
              <Button
                label={gt("Sign out")}
                tone="text"
                size="compact"
                disabled={state.busy}
                onPress={() => {
                  setServerUrl(null);
                  setUsername(null);
                  setPassword("");
                  void selfHostedSession.signOut();
                }}
              />
            </View>
            <Text className="text-sm text-foreground-muted">
              <T>Signing out removes this account's saved environments from this device.</T>
            </Text>
          </>
        ) : (
          <>
            <Text className="text-sm text-foreground-muted">
              <T>Sign in to your own connection server.</T>
            </Text>
            <AppTextInput
              accessibilityLabel={gt("Server URL")}
              placeholder="https://connect.example.com"
              value={address}
              onChangeText={setServerUrl}
              autoCapitalize="none"
              autoCorrect={false}
              keyboardType="url"
              editable={!state.busy}
            />
            <AppTextInput
              accessibilityLabel={gt("Username")}
              placeholder={gt("Username")}
              value={account}
              onChangeText={setUsername}
              autoCapitalize="none"
              autoCorrect={false}
              autoComplete="username"
              editable={!state.busy}
            />
            <AppTextInput
              accessibilityLabel={gt("Password")}
              placeholder={gt("Password")}
              value={password}
              onChangeText={setPassword}
              secureTextEntry
              autoCapitalize="none"
              autoCorrect={false}
              autoComplete="current-password"
              returnKeyType="go"
              editable={!state.busy}
              onSubmitEditing={() => {
                if (!state.busy && address.trim() && account.trim() && password) signIn();
              }}
            />
            <Button
              label={gt("Sign in")}
              disabled={state.busy || !address.trim() || !account.trim() || !password}
              loading={state.busy}
              onPress={signIn}
            />
          </>
        )}
        {state.error ? (
          <Text accessibilityLiveRegion="polite" className="text-sm text-danger-foreground">
            {errorDisplayMessage(state.error, messages) ?? gt("Connection failed.")}
          </Text>
        ) : null}
      </View>
    </View>
  );
}
