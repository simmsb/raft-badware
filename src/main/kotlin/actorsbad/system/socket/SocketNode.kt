package actorsbad.system.socket

import kotlinx.coroutines.*
import actorsbad.system.PID
import actorsbad.system.Process
import actorsbad.system.Registry
import actorsbad.system.RemoteNode
import actorsbad.system.socket.commands.Command
import actorsbad.system.utils.AsyncObjectInputStream
import actorsbad.system.utils.AsyncObjectOutputStream
import java.io.Serializable
import java.net.ConnectException
import java.net.InetAddress
import java.net.InetSocketAddress
import java.nio.channels.AsynchronousSocketChannel
import java.nio.channels.CompletionHandler
import kotlin.coroutines.Continuation
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine

class SocketNode(private val address: Pair<String, Int>,
                 private val sock: SocketMultiplexer) : RemoteNode {
    companion object {
        private val hostName: String by lazy { InetAddress.getLocalHost().hostAddress }

        private val backendPort: Int by lazy {
            System.getenv("BACKEND_PORT")?.toIntOrNull() ?: throw Exception("BACKEND_PORT was not set or invalid")
        }

        suspend fun connect(scope: CoroutineScope, host: String, port: Int): SocketNode {
            println("connecting to $host $port")
            val ss = tcpConnect(host, port)
            println("made tcp connection to $host $port")

            return withTimeoutOrNull(1000) {
                withContext(Dispatchers.IO) {
                    val oos = AsyncObjectOutputStream(ss)
                    val ois = AsyncObjectInputStream(ss)
                    val mp = SocketMultiplexer(scope, oos, ois)
                    mp.sendToInitiate(Pair(hostName, backendPort))
                    Registry.spawn(SocketServerHandler(mp))
                    SocketNode(Pair(host, port), mp)
                }
            } ?: throw ConnectException("Timed out connecting")
        }
    }

    /**
     * @return the ID of the remote node
     */
    override suspend fun id(): Long? {
        return try {
            this.sock.sendToInitiate(Command.GetID)
            this.sock.receiveFromInitiate() as? Long
        } catch (e: Exception) {
            null
        }
    }

    /**
     * Send a message to a pid on a remote node
     * @param <T> The type of the message (the other side will see this as Object)
     * @param pid The PID to send to
     * @param msg The msg to send
     * @return if sending succeeded
     */
    override suspend fun <T : Serializable> send(pid: PID, msg: T): Boolean {
        return try {
            this.sock.sendToInitiate(Command.SendPID(pid, msg))
            this.sock.receiveFromInitiate() as? Boolean ?: false
        } catch (e: Exception) {
            false
        }
    }

    /**
     * Send a message to a named process on a remote node
     * @param <T> The type of the message (the other side will see this as Object)
     * @param name The process name to send to
     * @param msg The msg to send
     * @return if sending succeeded
    </T> */
    override suspend fun <T : Serializable> send(name: String, msg: T): Boolean {
        return try {
            this.sock.sendToInitiate(Command.SendName(name, msg))
            this.sock.receiveFromInitiate() as? Boolean ?: false
        } catch (e: Exception) {
            false
        }
    }

    /**
     * Spawn a process on a remote node
     * @param <T> The type of the process
     * @param p The process to spawn
     * @return The PID of the remote process
    </T> */
    override suspend fun <T : Process> spawn(p: () -> T): PID? {
        return try {
            this.sock.sendToInitiate(Command.Spawn(p))
            this.sock.receiveFromInitiate() as? PID
        } catch (e: Exception) {
            null
        }
    }

    override suspend fun <T : Serializable> down(pid: PID, reason: T?): Boolean {
        return try {
            this.sock.sendToInitiate(Command.Down(pid, reason))
            this.sock.receiveFromInitiate() as? Boolean ?: false
        } catch (e: Exception) {
            false
        }
    }

    override suspend fun ping(nodes: List<Pair<String, Int>>): Boolean {
        return try {
            this.sock.sendToInitiate(Command.Ping(nodes))
            this.sock.receiveFromInitiate() as? Boolean ?: false
        } catch (e: Exception) {
            false
        }
    }

    override suspend fun lookupNamedProc(name: String): PID? {
        return try {
            this.sock.sendToInitiate(Command.LookupNamedProc(name))
            when (val r = this.sock.receiveFromInitiate()) {
                is PID -> r
                else -> null
            }
        } catch (e: Exception) {
            null
        }
    }

    override fun getAddress(): Pair<String, Int> = this.address
}

suspend fun tcpConnect(host: String, port: Int): AsynchronousSocketChannel {
    val sock = withContext(Dispatchers.IO) {
        @Suppress("BlockingMethodInNonBlockingContext")
        AsynchronousSocketChannel.open()
    }

    suspendCoroutine { it: Continuation<Unit> ->
        sock.connect(InetSocketAddress(host, port), it, ConnectCompletionHandler)
    }

    return sock
}

object ConnectCompletionHandler : CompletionHandler<Void, Continuation<Unit>> {
    override fun completed(result: Void?, attachment: Continuation<Unit>) {
        attachment.resume(Unit)
    }

    override fun failed(exc: Throwable, attachment: Continuation<Unit>) {
        attachment.resumeWithException(exc)
    }
}

