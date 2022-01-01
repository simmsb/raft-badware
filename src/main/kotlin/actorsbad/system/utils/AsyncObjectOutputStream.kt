package actorsbad.system.utils

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.io.ObjectOutputStream
import java.io.Serializable
import java.nio.channels.AsynchronousSocketChannel
import java.nio.channels.Channels

class AsyncObjectOutputStream(stream: AsynchronousSocketChannel) {
    private val outputStream = Channels.newOutputStream(stream)
    private val oos = ObjectOutputStream(outputStream)

    @Suppress("BlockingMethodInNonBlockingContext")
    suspend fun writeObject(o: Serializable) {
        withContext(Dispatchers.IO) {
            @Suppress("BlockingMethodInNonBlockingContext")
            oos.writeObject(o)
            oos.flush()
        }
    }
}