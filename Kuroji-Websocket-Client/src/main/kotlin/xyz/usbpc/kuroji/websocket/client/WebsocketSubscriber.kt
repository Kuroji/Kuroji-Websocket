package xyz.usbpc.kuroji.websocket.client

import kotlinx.coroutines.experimental.future.await
import kotlinx.coroutines.experimental.sync.Mutex
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.x.async.AsyncCuratorFramework
import org.apache.curator.x.async.api.CreateOption
import org.apache.zookeeper.CreateMode
import xyz.astolfo.websocket.rpc.KurojiWebsocket
import java.net.InetAddress
import java.util.concurrent.TimeUnit

class WebsocketSubscriber constructor(val port: Int, client: CuratorFramework){
    val aclient: AsyncCuratorFramework
    init {
        try {
            client.blockUntilConnected(30, TimeUnit.SECONDS)
        } catch (ex: InterruptedException) {
            //TODO: Make exception less generic
            throw Exception("Could not connect with zookeeper after 30 seconds", ex)
        }
        aclient = AsyncCuratorFramework.wrap(client)
    }
    constructor(port: Int, zookeeper: String = "localhost:2181") : this(port, CuratorFrameworkFactory.newClient(zookeeper, ExponentialBackoffRetry(1000, 3)))

    var subData = KurojiWebsocket.Subscriber.newBuilder()
            .setHostname(InetAddress.getLocalHost().hostName)
            .setPort(port)
    var ourUrl: String? = null
    suspend fun register() {
        ourUrl = aclient.create()
                .withOptions(setOf(CreateOption.createParentsAsContainers), CreateMode.EPHEMERAL_SEQUENTIAL)
                .forPath("/websocket/clients/sub", subData.build().toByteArray())
                .await()
    }

    suspend fun unregister() {
        if (ourUrl != null) {
            aclient.delete().forPath(ourUrl).await()
            ourUrl = null
        }
    }

    suspend fun shutdown() {
        unregister()
    }
}