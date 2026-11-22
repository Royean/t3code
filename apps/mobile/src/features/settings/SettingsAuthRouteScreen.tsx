import { useAuth } from "@clerk/expo";
import {
  AuthView,
  type UserProfileCustomPage,
  UserProfileView,
} from "@clerk/expo/native-components";
import { StackActions, useNavigation, type StaticScreenProps } from "@react-navigation/native";
import { useGT } from "gt-react-native";
import { useEffect, useState } from "react";
import { View } from "react-native";
import { useSafeAreaInsets } from "react-native-safe-area-context";

import { AppBar } from "../../components/ui/AppBar";
import { Button } from "../../components/ui/Button";
import { ScreenScrollView } from "../../components/ui/ScreenScrollView";
import { useHardwareBack } from "../../lib/backHandler";
import { hasCloudPublicConfig } from "../cloud/publicConfig";
import { T3ConnectProfilePage } from "../cloud/T3ConnectProfilePage";
import { SelfHostedConnectSection } from "../connection/SelfHostedConnectSection";
import { workspaceHasColumns } from "../layout/workspace/workspace-selection-store";
import { openSettings } from "./settings-navigation";

const USER_PROFILE_CUSTOM_PAGES = [
  {
    path: "t3-connect",
    label: "T3 Connect",
    icon: "globe",
    content: <T3ConnectProfilePage />,
  },
] satisfies UserProfileCustomPage[];

export function SettingsAuthRouteScreen({
  route,
}: StaticScreenProps<{ method?: "official" } | undefined>) {
  const gt = useGT();
  const navigation = useNavigation();
  const cloudConfigured = hasCloudPublicConfig();
  const [showSelfHosted, setShowSelfHosted] = useState(false);
  const back = () => {
    if (showSelfHosted) setShowSelfHosted(false);
    else navigation.goBack();
  };
  useHardwareBack(showSelfHosted, () => {
    setShowSelfHosted(false);
    return true;
  });
  const connected = () => {
    const state = navigation.getState();
    if (!state || !navigation.isFocused()) return;
    const { routes, index } = state;
    const previous = routes[index - 1]?.name;
    if (previous !== "ConnectionsNew" && previous !== "SettingsEnvironmentNew") return;
    if (workspaceHasColumns() && routes[index - 2]?.name === "Settings") {
      openSettings(navigation, "SettingsEnvironments");
    } else if (index > 1) {
      navigation.dispatch(StackActions.pop(2));
    } else {
      navigation.dispatch(StackActions.popTo("Home"));
    }
  };

  if (cloudConfigured && !showSelfHosted) {
    return (
      <ConfiguredSettingsAuthRouteScreen
        onBack={back}
        onSelfHosted={
          route.params?.method === "official" ? undefined : () => setShowSelfHosted(true)
        }
      />
    );
  }

  return (
    <View className="flex-1 bg-sheet">
      <AppBar title={gt("T3 Account")} onBack={back} />
      <ScreenScrollView
        className="flex-1"
        contentContainerClassName="px-5 py-4"
        keyboardShouldPersistTaps="handled"
      >
        <SelfHostedConnectSection onConnected={connected} />
      </ScreenScrollView>
    </View>
  );
}

export function SettingsOfficialAuthRouteScreen() {
  const navigation = useNavigation();
  return hasCloudPublicConfig() ? (
    <ConfiguredSettingsAuthRouteScreen onBack={() => navigation.goBack()} />
  ) : null;
}

function ConfiguredSettingsAuthRouteScreen({
  onBack,
  onSelfHosted,
}: {
  readonly onBack: () => void;
  readonly onSelfHosted?: () => void;
}) {
  const gt = useGT();
  const { bottom } = useSafeAreaInsets();
  const { isLoaded, isSignedIn } = useAuth({ treatPendingAsSignedOut: false });
  // Once signed in, stay on the profile view so signing out pops back instead of
  // flashing the sign-in form.
  const [hasBeenSignedIn, setHasBeenSignedIn] = useState(isSignedIn === true);
  if (isSignedIn && !hasBeenSignedIn) setHasBeenSignedIn(true);

  // Clerk reports sign-out only through its auth store (the native profile view
  // and the T3 Connect page can both sign out), so this follows that store.
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- pops the navigator when Clerk's auth store reports a sign-out from the profile view or Connect page
  useEffect(() => {
    if (hasBeenSignedIn && isLoaded && isSignedIn === false) {
      onBack();
    }
  }, [hasBeenSignedIn, isLoaded, isSignedIn, onBack]);

  return (
    <View collapsable={false} className="flex-1 overflow-hidden bg-sheet">
      <View className="flex-1">
        {isLoaded ? (
          hasBeenSignedIn ? (
            <UserProfileView
              customPages={USER_PROFILE_CUSTOM_PAGES}
              isDismissible={false}
              onHostBack={onBack}
            />
          ) : (
            <AuthView isDismissible={false} onHostBack={onBack} />
          )
        ) : null}
      </View>
      {onSelfHosted ? (
        <View className="px-5 pt-2" style={{ paddingBottom: Math.max(bottom, 8) }}>
          <Button
            label={gt("Self-hosted connection")}
            onPress={onSelfHosted}
            tone="text"
            fullWidth
          />
        </View>
      ) : null}
    </View>
  );
}
