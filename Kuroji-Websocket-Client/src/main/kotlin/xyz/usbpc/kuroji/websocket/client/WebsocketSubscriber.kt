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

class WebsocketSubscriber(port: Int, zookeeper: String = "localhost:2181") {
    val aclient: AsyncCuratorFramework
    val client: CuratorFramework
    var subData = KurojiWebsocket.Subscriber.newBuilder()
            .setHostname(InetAddress.getLocalHost().hostName)
            .setPort(port)
    var ourUrl: String? = null
    init {
        val retryPolicy = ExponentialBackoffRetry(1000, 3)
        client = CuratorFrameworkFactory.newClient(zookeeper, retryPolicy)
        client.start()
        try {
            client.blockUntilConnected(30, TimeUnit.SECONDS)
        } catch (ex: InterruptedException) {
            //TODO: Make exception less generic
            throw Exception("Could not connect with zookeeper after 30 seconds", ex)
        }
        aclient = AsyncCuratorFramework.wrap(client)
    }
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
        aclient.unwrap().close()
    }
}