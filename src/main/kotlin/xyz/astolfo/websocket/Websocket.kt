package xyz.astolfo.websocket

import com.github.salomonbrys.kotson.fromJson
import com.github.salomonbrys.kotson.jsonArray
import com.github.salomonbrys.kotson.jsonObject
import com.google.gson.Gson
import com.google.gson.JsonElement
import com.google.gson.JsonObject
import com.google.gson.JsonPrimitive
import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.launch
import okhttp3.*
import okio.ByteString
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.Bean
import java.io.ByteArrayOutputStream
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.zip.Inflater
import java.util.zip.InflaterOutputStream

@SpringBootApplication
class WebsocketApp {
    @Bean
    fun startWebSocket() = ShardManager(0 until 2, 2, System.getenv("token"))
}

fun main(args: Array<String>) {
    val app = SpringApplication(WebsocketApp::class.java)
    app.isWebEnvironment = false
    app.setLogStartupInfo(false)
    app.run(*args)
}

enum class Opcode {
    DISPATCH,
    HEARTBEAT,
    IDENTIFY,
    STATUS_UPDATE,
    VOICE_STATE_UPDATE,
    VOICE_SERVER_PING,
    RESUME,
    RECONNECT,
    REQUEST_GUILD_MEMBERS,
    INVALID_SESSION,
    HELLO,
    HEARTBEAT_ACK
}

class ShardManager(shardRangeIds: IntRange,
                   val total: Int,
                   private val botToken: String) {

    val shardRange = IntRange(shardRangeIds.start.coerceAtLeast(0),
            shardRangeIds.last.coerceAtMost(total - 1))
    private val connectQueue = ArrayDeque<Int>()

    private val shard = mutableMapOf<Int, DiscordWebsocket>()

    private val scheduler = Executors.newSingleThreadScheduledExecutor()

    init {
        shardRange.forEach { id ->
            shard[id] = DiscordWebsocket(botToken, id, total, { event, data ->
                println("[GET] [$id] [$event] $data")
            }, connectQueue)
        }
        scheduler.scheduleWithFixedDelay({
            val shardId = connectQueue.poll() ?: return@scheduleWithFixedDelay
            println("Starting Shard: $shardId")
            shard[shardId]!!.connect()
        }, 0, 6, TimeUnit.SECONDS)
    }
}

class DiscordWebsocket(private val botToken: String,
                       val id: Int,
                       val total: Int,
                       private val eventConsumer: (String, String) -> Unit,
                       private val connectQueue: Queue<Int>) : WebSocketListener() {

    private val gatewayRequest = Request.Builder().url("wss://gateway.discord.gg/?v=6&encoding=json&compress=zlib-stream").build()
    private val okHttpClient = OkHttpClient()
    private val gson = Gson()
    private lateinit var inflater: Inflater
    private lateinit var webSocket: WebSocket
    private val heartbeatChecker = HeartbeatChecker({
        send(Opcode.HEARTBEAT, JsonPrimitive(lastSequence))
    }, this::reconnect)
    private var lastSequence: Long = 0
    private var sessionId = ""

    init {
        queueConnect()
    }

    private fun queueConnect() {
        connectQueue.add(id)
    }

    fun connect() {
        webSocket = okHttpClient.newWebSocket(gatewayRequest, this)
    }

    private fun reconnect() {
        launch {
            webSocket.close(1000, "Reconnect")
            queueConnect()
        }
    }

    override fun onOpen(webSocket: WebSocket?, response: Response?) {
        inflater = Inflater()
    }

    override fun onMessage(webSocket: WebSocket, bytes: ByteString) {
        val byteArray = bytes.toByteArray()

        val out = ByteArrayOutputStream(byteArray.size * 2)
        InflaterOutputStream(out, inflater).use {
            it.write(byteArray)
        }
        onMessage(webSocket, out.toString("UTF-8"))
    }

    override fun onMessage(webSocket: WebSocket, text: String) {
        try {
            val json = gson.fromJson<JsonObject>(text)

            json["s"]?.takeIf { !it.isJsonNull }?.asLong?.let { lastSequence = it }

            val data = json["d"]?.takeIf { it.isJsonObject }?.asJsonObject
            val opcode = Opcode.values()[json["op"].asInt]
            when (opcode) {
                Opcode.HELLO -> {
                    identify()

                    val interval = data!!["heartbeat_interval"].asLong
                    heartbeatChecker.startHeartbeat(interval)
                }
                Opcode.INVALID_SESSION -> {
                    val valid = json["d"].asBoolean
                    launch {
                        delay(5, TimeUnit.SECONDS)
                        when (valid) {
                            true -> identify()
                            false -> resume()
                        }
                    }
                }
                Opcode.HEARTBEAT_ACK -> heartbeatChecker.consumeBeat()
                Opcode.DISPATCH -> {
                    val event = json["t"].asString
                    if (event == "READY")
                        sessionId = data!!["session_id"].asString
                    eventConsumer.invoke(event, data.toString())
                }
                Opcode.RECONNECT -> reconnect()
                else -> TODO("incoming Opcode ${opcode.name} isn't supported yet!")
            }
        } catch (e: Exception) {
            println("[GET] [$id] $text")
            throw e
        }
    }

    override fun onFailure(webSocket: WebSocket?, t: Throwable?, response: Response?) {
        t?.printStackTrace()
        reconnect()
    }

    override fun onClosed(webSocket: WebSocket?, code: Int, reason: String?) {
        heartbeatChecker.stop()
        when (code) {
            4004 -> error("Tried to connect with an invalid token")
            4010 -> error("Invalid sharding data, check your client options")
            4011 -> error("Shard would be on over 2500 guilds. Add more shards")
            4009 -> launch {
                // Invalid session
                sessionId = ""
                queueConnect()
            }
            1000 -> println("Session closed by bot")
        }
    }

    private fun send(op: Opcode, d: JsonElement) =
            webSocket.send(jsonObject(
                    "op" to op.ordinal,
                    "d" to d
            ).toString().apply {
                println("[SEND] [$id] $this")
            })

    private fun identify() {
        if (sessionId.isNotBlank())
            resume()
        else
            send(Opcode.IDENTIFY,
                    jsonObject(
                            "token" to botToken,
                            "properties" to jsonObject(
                                    "\$os" to System.getProperty("os.name"),
                                    "\$browser" to "Astolfo",
                                    "\$device" to "Astolfo",
                                    "\$referring_domain" to "",
                                    "\$referrer" to ""
                            ),
                            "presence" to jsonObject(
                                    "game" to jsonObject(
                                            "name" to "Astolfo 2.0 Testing",
                                            "type" to 0
                                    ),
                                    "status" to "online",
                                    "since" to 0,
                                    "afk" to false
                            ),
                            "shard" to jsonArray(id, total),
                            "compress" to true,
                            "large_threshold" to 250,
                            "v" to 6
                    ))
    }

    private fun resume() {
        send(Opcode.RESUME, jsonObject(
                "seq" to lastSequence,
                "token" to botToken,
                "session_id" to sessionId
        ))
    }

}