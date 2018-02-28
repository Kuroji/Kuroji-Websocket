import com.google.gson.GsonBuilder
import io.grpc.ServerBuilder
import io.grpc.stub.StreamObserver
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.newSingleThreadContext
import kotlinx.coroutines.experimental.runBlocking
import xyz.astolfo.websocket.rpc.KurojiWebsocket
import xyz.astolfo.websocket.rpc.WebsocketSubscriberGrpc
import xyz.usbpc.kuroji.websocket.client.WebsocketSubscriber
import kotlin.concurrent.thread

fun main(args: Array<String>) = runBlocking {
    val port = 8120

    //Lets start up grpc
    val server = ServerBuilder.forPort(port)
            .addService(TestWebsocketSubscriber())
            .build()
            .start()
    val wsSub = WebsocketSubscriber(port)
    wsSub.register()

    Runtime.getRuntime().addShutdownHook(
            thread (start = false, isDaemon = false) {
                runBlocking(newSingleThreadContext("Shutdown")) {
                    wsSub.shutdown()
                    server.shutdown()
                }
            }
    )
    server.awaitTermination()
}

class TestWebsocketSubscriber : WebsocketSubscriberGrpc.WebsocketSubscriberImplBase() {
    private val gson = GsonBuilder().setPrettyPrinting().create()
    override fun onEvent(request: KurojiWebsocket.RawEvent, responseObserver: StreamObserver<KurojiWebsocket.SubResponse>) {
        launch {
            println(gson.toJson(request))
            responseObserver.onNext(KurojiWebsocket.SubResponse.getDefaultInstance())
            responseObserver.onCompleted()
        }
    }
}