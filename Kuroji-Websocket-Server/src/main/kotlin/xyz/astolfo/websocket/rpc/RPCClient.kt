package xyz.astolfo.websocket.rpc

import io.grpc.*
import io.grpc.util.RoundRobinLoadBalancerFactory
import kotlinx.coroutines.experimental.Job
import kotlinx.coroutines.experimental.future.await
import kotlinx.coroutines.experimental.guava.await
import kotlinx.coroutines.experimental.launch
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.x.async.AsyncCuratorFramework
import org.apache.zookeeper.Watcher
import java.net.InetSocketAddress
import java.net.URI

class RPCClient(zooAddr: String) {
    private val zoo: CuratorFramework
    private lateinit var channel : ManagedChannel
    private lateinit var stub : WebsocketSubscriberGrpc.WebsocketSubscriberFutureStub
    init {
        //This is a thing the CuratorFramework needs in order to know how to try to reconnect in case the connection
        //fails.
        val retryPolicy = ExponentialBackoffRetry(1000, 3)
        //Let's create the connection to zookeeper so we can get the list of clients
        zoo = CuratorFrameworkFactory.newClient(zooAddr, retryPolicy)
        zoo.start()
    }

    /**
     * Starts the sending of events also starts up everything needed
     */
    fun start() {
        val nameResover = ZookeeperNameResolverProvider(AsyncCuratorFramework.wrap(zoo.usingNamespace("websocket")))
        //This creates a logical channel for grpc so we can send stuff over it
        //A logical channel can have many connection that are long lived
        channel = ManagedChannelBuilder
                .forTarget("/clients")
                .loadBalancerFactory(RoundRobinLoadBalancerFactory.getInstance())
                .nameResolverFactory(nameResover)
                //TODO make secure
                .usePlaintext(true)
                .build()
        //Now let's create a stub that we can actually use to send data
        stub = WebsocketSubscriberGrpc.newFutureStub(channel)
    }

    fun sentEvent(type: String, data: String) {
        //TODO this should probably be in some special sending context... but whatever
        launch {
            val builder = KurojiWebsocket.RawEvent.newBuilder()
            builder.name = type
            builder.data = data
            //TODO: Only placeholders... put the real things in please
            builder.botId = 123L
            builder.shardId = 1
            builder.traceId = "Tracing..."
            stub.onEvent(builder.build()).await()
        }
    }

    fun shutdown() {
        zoo.close()
        channel.shutdown()
    }
}

/**
 * Just factory pattern things cause it was written for java and stuff
 */
class ZookeeperNameResolverProvider(val aclient: AsyncCuratorFramework) : NameResolverProvider() {
    override fun newNameResolver(targetUri: URI, params: Attributes): NameResolver {
        return ZookeeperNameResolver(targetUri.toString(), aclient)
    }

    override fun getDefaultScheme(): String {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun priority(): Int {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun isAvailable(): Boolean {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
    /**
     * This gets the addresses of the servers that want to get events out of zookeeper if we feed it the right parameters
     */
    class ZookeeperNameResolver(val dir: String, val aclient: AsyncCuratorFramework) : NameResolver() {
        private var job: Job? = null
        override fun shutdown() {
            job?.cancel()
            job = null
        }

        override fun start(listener: Listener) {
            job = launch {
                while (isActive) {
                    val thing = aclient.watched().children.forPath(dir)
                    val subs = thing.await()
                            .map {server ->
                                aclient.data.forPath("$dir/$server").await()
                            }.map {data ->
                                KurojiWebsocket.Subscriber.parseFrom(data)
                            }.map {sub ->
                                EquivalentAddressGroup(InetSocketAddress(sub.hostname, sub.port), Attributes.EMPTY)
                            }
                    listener.onAddresses(subs, Attributes.EMPTY)
                    if (Watcher.Event.EventType.NodeDeleted == thing.event().await().type)
                        break
                }
            }
        }

        override fun getServiceAuthority(): String {
            //TODO Figure out what this does
            return "none"
        }
    }

}