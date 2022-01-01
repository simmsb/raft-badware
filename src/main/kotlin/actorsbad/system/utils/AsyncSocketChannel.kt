package actorsbad.system.utils

import java.nio.ByteBuffer
import java.nio.channels.AsynchronousSocketChannel
import java.nio.channels.CompletionHandler
import java.util.concurrent.TimeUnit
import kotlin.coroutines.Continuation
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine

class AsyncSocketChannel(val channel: AsynchronousSocketChannel) {
    suspend fun read(buf: ByteBuffer): Int {
        return this.channel.asyncRead(buf)
    }

    private suspend fun AsynchronousSocketChannel.asyncRead(buf: ByteBuffer): Int {
        return suspendCoroutine {
            this.read(buf, it, RWCompletionHandler)
        }
    }

    suspend fun write(buf: ByteBuffer): Int {
        return this.channel.asyncWrite(buf)
    }

    suspend fun write(bufs: Array<ByteBuffer>): Long {
        return this.channel.asyncWrite(bufs)
    }

    private suspend fun AsynchronousSocketChannel.asyncWrite(buf: ByteBuffer): Int {
        return suspendCoroutine {
            this.write(buf, it, RWCompletionHandler)
        }
    }

    private suspend fun AsynchronousSocketChannel.asyncWrite(bufs: Array<ByteBuffer>): Long {
        return suspendCoroutine {
            this.write(bufs, 0, bufs.size, -1, TimeUnit.SECONDS, it, RWLCompletionHandler)
        }
    }

    object RWCompletionHandler : CompletionHandler<Int, Continuation<Int>> {
        override fun completed(result: Int, attachment: Continuation<Int>) {
            attachment.resume(result)
        }

        override fun failed(exc: Throwable, attachment: Continuation<Int>) {
            attachment.resumeWithException(exc)
        }
    }

    object RWLCompletionHandler : CompletionHandler<Long, Continuation<Long>> {
        override fun completed(result: Long, attachment: Continuation<Long>) {
            attachment.resume(result)
        }

        override fun failed(exc: Throwable, attachment: Continuation<Long>) {
            attachment.resumeWithException(exc)
        }
    }

    fun close() {
        this.channel.close()
    }
}

