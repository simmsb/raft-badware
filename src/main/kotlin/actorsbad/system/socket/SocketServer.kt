package actorsbad.system.socket

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.collect
import actorsbad.system.Process
import actorsbad.system.Registry
import actorsbad.system.utils.AsyncObjectInputStream
import actorsbad.system.utils.AsyncObjectOutputStream
import actorsbad.system.utils.AsyncSocketServer
import java.nio.channels.AsynchronousSocketChannel
import kotlin.coroutines.coroutineContext

class SocketServer(private val hostname: String, private val port: Int) : Process() {
    private suspend fun handle(chan: AsynchronousSocketChannel) {
        val oos = AsyncObjectOutputStream(chan)
        val ois = AsyncObjectInputStream(chan)
        val mp = SocketMultiplexer(CoroutineScope(coroutineContext), oos, ois)
        val theirAddr = mp.receiveFromInitiator() as? Pair<*, *>
                ?: throw Exception("We got sent an address that wasn't an address")
        val theirHost = theirAddr.first as? String ?: throw Exception("We got a host that wasn't a String")
        val theirPort = theirAddr.second as? Int ?: throw Exception("We got a port that wasn't an Int")
        Registry.spawn(SocketServerHandler(mp))
        Registry.addRemoteNode(SocketNode(Pair(theirHost, theirPort), mp))
    }

    override suspend fun mainFn() {
        val ss = AsyncSocketServer(this.hostname, this.port)

        coroutineScope {
            ss.acceptFlow().collect {
                try {
                    this@SocketServer.handle(it)
                } catch (e: Exception) {
                    print("Failed to start client with: $e")
                }
            }
        }
    }
}