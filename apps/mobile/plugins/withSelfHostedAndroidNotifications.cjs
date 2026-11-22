const { AndroidConfig, withAndroidManifest } = require("expo/config-plugins");

module.exports = function withSelfHostedAndroidNotifications(config) {
  return withAndroidManifest(config, (next) => {
    const manifest = next.modResults.manifest;
    manifest.$["xmlns:tools"] = "http://schemas.android.com/tools";
    const application = AndroidConfig.Manifest.getMainApplicationOrThrow(next.modResults);
    // expo-notifications also provides local permissions and channels. Its Firebase
    // dependency is shared by upstream builds, but cannot initialize in the fork.
    for (const [type, names] of Object.entries({
      provider: ["com.google.firebase.provider.FirebaseInitProvider"],
      receiver: ["com.google.firebase.iid.FirebaseInstanceIdReceiver"],
      service: [
        "com.google.firebase.messaging.FirebaseMessagingService",
        "expo.modules.t3agentnotifications.AgentMessagingService",
      ],
    })) {
      application[type] ??= [];
      for (const name of names) {
        if (!application[type].some((entry) => entry.$["android:name"] === name)) {
          const attributes = { "android:name": name, "tools:node": "remove" };
          // Lint treats this removal marker as a registered service in the source manifest.
          if (name === "expo.modules.t3agentnotifications.AgentMessagingService") {
            attributes["tools:ignore"] = "Instantiatable";
          }
          application[type].push({ $: attributes });
        }
      }
    }
    return next;
  });
};
