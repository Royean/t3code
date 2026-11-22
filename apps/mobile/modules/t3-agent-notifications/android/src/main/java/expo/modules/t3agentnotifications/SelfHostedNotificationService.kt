package expo.modules.t3agentnotifications

import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.app.Service
import android.content.Context
import android.content.Intent
import android.content.pm.ServiceInfo
import android.os.Build
import android.os.Handler
import android.os.IBinder
import android.os.Looper
import androidx.core.app.NotificationCompat
import androidx.core.app.ServiceCompat
import androidx.core.content.ContextCompat
import org.json.JSONException
import org.json.JSONObject
import java.io.IOException
import java.net.URI
import javax.net.ssl.HttpsURLConnection

/** Receives account notifications even when React Native is suspended. */
class SelfHostedNotificationService : Service() {
  companion object {
    private const val CHANNEL = "self-hosted-notifications"
    private const val NOTIFICATION = 73002

    @Volatile var listening = false
      private set

    fun connect(context: Context, origin: String, token: String): Boolean = try {
      ContextCompat.startForegroundService(context, Intent(context, SelfHostedNotificationService::class.java)
        .putExtra("origin", origin)
        .putExtra("deliveryToken", token))
      true
    } catch (_: IllegalStateException) {
      // The app may have gone to the background during registration. Resume on foreground.
      false
    } catch (_: SecurityException) {
      false
    }

    fun disconnect(context: Context) {
      context.stopService(Intent(context, SelfHostedNotificationService::class.java))
    }
  }

  @Volatile private var listener: Listener? = null

  override fun onBind(intent: Intent?): IBinder? = null

  override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
    val origin = intent?.getStringExtra("origin")
    val token = intent?.getStringExtra("deliveryToken")
    val uri = origin?.let { runCatching { URI(it) }.getOrNull() }
    if (uri == null || uri.scheme != "https" || uri.host == null || uri.userInfo != null ||
      !uri.rawPath.isNullOrEmpty() || uri.rawQuery != null || uri.rawFragment != null ||
      token == null || !Regex("[A-Za-z0-9_-]{43}").matches(token)
    ) {
      stopSelf(startId)
      return START_NOT_STICKY
    }
    try {
      showConnectionNotification()
    } catch (_: IllegalStateException) {
      stopSelf(startId)
      return START_NOT_STICKY
    } catch (_: SecurityException) {
      stopSelf(startId)
      return START_NOT_STICKY
    }
    val previous = listener
    val endpoint = uri.resolve("/v1/mobile/notifications")
    if (previous != null && previous.matches(endpoint, token)) {
      previous.startId = startId
      return START_NOT_STICKY
    }
    listener = Listener(endpoint, token, startId)
    listening = true
    previous?.cancel()
    listener?.start()
    // The credential lives in memory. An explicit app launch starts a new authorized connection.
    return START_NOT_STICKY
  }

  override fun onDestroy() {
    val previous = listener
    listener = null
    listening = false
    previous?.cancel()
    super.onDestroy()
  }

  private fun showConnectionNotification() {
    val title = getString(R.string.self_hosted_notifications)
    if (Build.VERSION.SDK_INT >= 26) {
      getSystemService(NotificationManager::class.java).createNotificationChannel(
        NotificationChannel(CHANNEL, title, NotificationManager.IMPORTANCE_LOW),
      )
    }
    val launch = packageManager.getLaunchIntentForPackage(packageName)?.let {
      PendingIntent.getActivity(this, NOTIFICATION, it, PendingIntent.FLAG_IMMUTABLE)
    }
    val notification = NotificationCompat.Builder(this, CHANNEL)
      .setSmallIcon(R.drawable.agent_activity_mark)
      .setContentTitle(applicationInfo.loadLabel(packageManager))
      .setContentText(title)
      .setContentIntent(launch)
      .setOngoing(true)
      .setSilent(true)
      .build()
    ServiceCompat.startForeground(
      this, NOTIFICATION, notification,
      if (Build.VERSION.SDK_INT >= 34) ServiceInfo.FOREGROUND_SERVICE_TYPE_SPECIAL_USE else 0,
    )
  }

  private inner class Listener(private val endpoint: URI, private val token: String, @Volatile var startId: Int) :
    Thread("t3-self-hosted-notifications") {
    @Volatile private var request: HttpsURLConnection? = null

    fun matches(uri: URI, credential: String) = isAlive && endpoint == uri && token == credential

    fun cancel() {
      interrupt()
      request?.disconnect()
    }

    override fun run() {
      var after = 0L
      var replay = true
      var retryDelay = 1_000L
      while (listener === this && !isInterrupted) {
        try {
          val connection = endpoint.toURL().openConnection() as HttpsURLConnection
          request = connection
          try {
            connection.instanceFollowRedirects = false
            connection.requestMethod = "POST"
            connection.connectTimeout = 10_000
            connection.readTimeout = 30_000
            connection.doOutput = true
            connection.setRequestProperty("Authorization", "Bearer $token")
            connection.setRequestProperty("Content-Type", "application/json")
            if (listener !== this || isInterrupted) return
            connection.outputStream.use { it.write("{\"after\":$after,\"replay\":$replay}".toByteArray(Charsets.UTF_8)) }
            val status = connection.responseCode
            if (status == 401 || status == 404) {
              Handler(Looper.getMainLooper()).post {
                if (listener === this) {
                  listening = false
                  AgentNotifications.clear(this@SelfHostedNotificationService)
                  stopSelf(startId)
                }
              }
              return
            }
            if (status != 200) throw IOException("Notification sync failed")
            val body = connection.inputStream.bufferedReader().use { reader ->
              val data = CharArray(8_192)
              val response = StringBuilder()
              while (true) {
                val count = reader.read(data)
                if (count == -1) break
                if (response.length + count > 128 * 1024) throw IOException("Notification response too large")
                response.append(data, 0, count)
              }
              response.toString()
            }
            val messages = JSONObject(body).getJSONArray("messages")
            for (index in 0 until messages.length()) {
              if (listener !== this || isInterrupted) return
              val message = messages.getJSONObject(index)
              val id = message.getLong("id")
              if (id <= after) continue
              val data = message.getJSONObject("data")
              AgentNotifications.receive(this@SelfHostedNotificationService, data.keys().asSequence().associateWith(data::getString))
              after = id
            }
            replay = false
            retryDelay = 1_000L
            continue
          } finally {
            connection.disconnect()
            request = null
          }
        } catch (_: IOException) {
          // The cursor is only advanced after the message has been presented locally.
        } catch (_: JSONException) {
          // An invalid response is retried with backoff as well.
        }
        replay = true
        try {
          sleep(retryDelay)
        } catch (_: InterruptedException) {
          return
        }
        retryDelay = (retryDelay * 2).coerceAtMost(30_000L)
      }
    }
  }
}
