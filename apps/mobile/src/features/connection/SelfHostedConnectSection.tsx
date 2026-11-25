import { useAtomValue } from "@effect/atom-react";
import { errorDisplayMessage } from "@t3tools/client-runtime/errors";
import { EnvironmentId } from "@t3tools/contracts";
import { hasRouteToOrigin } from "@t3tools/client-runtime/connection";
import Constants from "expo-constants";
import { T, useGT, useMessages } from "gt-react-native";
import { useEffect, useState, useSyncExternalStore } from "react";
import { View } from "react-native";

import { AppText as Text, AppTextInput } from "../../components/ui/AppText";
import { Button } from "../../components/ui/Button";
import * as Dialog from "../../components/ui/Dialog";
import { ListActionRow } from "../../components/ui/ListActionRow";
import { ListRow } from "../../components/ui/ListRow";
import { ListRowLayout } from "../../components/ui/ListRowLayout";
import { ListSection } from "../../components/ui/ListSection";
import { environmentCatalog } from "../../connection/catalog";
import { selfHostedOnly, selfHostedSession } from "./selfHostedConnection";

const SELF_HOSTED_SERVER_URL =
  (Constants.expoConfig?.extra?.selfHostedAccountOrigin as string | null | undefined) ||
  "https://t3.vivid-dream.online";

export function SelfHostedConnectSection({ onConnected }: { readonly onConnected?: () => void }) {
  const gt = useGT();
  const messages = useMessages();
  const state = useSyncExternalStore(selfHostedSession.subscribe, selfHostedSession.getSnapshot);
  const catalog = useAtomValue(environmentCatalog.catalogValueAtom);
  const [username, setUsername] = useState<string | null>(null);
  const [password, setPassword] = useState("");
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- restores encrypted account credentials when entering connection settings
  useEffect(() => {
    void selfHostedSession.restore();
  }, []);
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- refreshes the authenticated device list when the account screen opens
  useEffect(() => {
    if (state.signedIn) void selfHostedSession.refreshDevices();
  }, [state.signedIn]);

  const account = username ?? (state.serverUrl === SELF_HOSTED_SERVER_URL ? state.username : "");
  const signIn = () => {
    const attempt = { serverUrl: SELF_HOSTED_SERVER_URL, username: account, password };
    setPassword("");
    void selfHostedSession.login(attempt);
  };

  return (
    <View collapsable={false} className="mt-5 gap-2">
      <Text className="px-4 text-sm font-t3-medium text-foreground-muted">
        {selfHostedOnly ? "T3 Connect" : gt("Self-hosted connection")}
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
              const entry = catalog.entries.get(EnvironmentId.make(environment.environmentId));
              const saved = entry !== undefined && hasRouteToOrigin(entry, environment.httpBaseUrl);
              return (
                <ListRow
                  key={environment.environmentId}
                  label={environment.label}
                  description={environment.httpBaseUrl}
                  detail={
                    !environment.available ? gt("Offline") : saved ? gt("Saved") : gt("Connect")
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
                if (!state.busy && account.trim() && password) signIn();
              }}
            />
            <Button
              label={gt("Sign in")}
              disabled={state.busy || !account.trim() || !password}
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
      {state.devices ? (
        <ListSection title={gt("Devices")}>
          {state.devices.map((device) => {
            const label = ["T3 Pro", "T4 code", "t4code"].includes(device.label ?? "")
              ? gt("Desktop session")
              : (device.label ?? gt("Device"));
            return device.current ? (
              <ListRowLayout key={device.id} label={label} detail={gt("This device")} />
            ) : (
              <ListActionRow
                key={device.id}
                label={label}
                detail={gt("Sign out")}
                disabled={state.busy}
                onPress={() =>
                  Dialog.alert(gt("Sign out?"), label, [
                    { text: gt("Cancel"), style: "cancel" },
                    {
                      text: gt("Sign out"),
                      style: "destructive",
                      onPress: () => void selfHostedSession.revokeDevice(device.id),
                    },
                  ])
                }
              />
            );
          })}
        </ListSection>
      ) : null}
    </View>
  );
}
