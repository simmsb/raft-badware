package actorsbad.system.utils

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import java.net.InetSocketAddress
import java.nio.channels.AsynchronousServerSocketChannel
import java.nio.channels.AsynchronousSocketChannel
import java.nio.channels.CompletionHandler
import kotlin.coroutines.Continuation
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine

class AsyncSocketServer(host: String, port: Int) {
    private val socketServer = AsynchronousServerSocketChannel.open()!!

    init {
        socketServer.bind(InetSocketAddress(host, port))
    }

    suspend fun accept(): AsynchronousSocketChannel {
        return this.socketServer.asyncAccept()
    }

    fun acceptFlow(): Flow<AsynchronousSocketChannel> = flow {
        while (true) {
            emit(accept())
        }
    }

    private suspend fun AsynchronousServerSocketChannel.asyncAccept(): AsynchronousSocketChannel {
        return suspendCoroutine {
            this.accept(it, AcceptCompletionHandler)
        }
    }

    object AcceptCompletionHandler : CompletionHandler<AsynchronousSocketChannel, Continuation<AsynchronousSocketChannel>> {
        override fun completed(result: AsynchronousSocketChannel, attachment: Continuation<AsynchronousSocketChannel>) {
            attachment.resume(result)
        }

        override fun failed(exc: Throwable, attachment: Continuation<AsynchronousSocketChannel>) {
            attachment.resumeWithException(exc)
        }
    }
}