package actorsbad.system.utils

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.io.ObjectInputStream
import java.nio.channels.AsynchronousSocketChannel
import java.nio.channels.Channels

class AsyncObjectInputStream(stream: AsynchronousSocketChannel) {
    private val inputStream = Channels.newInputStream(stream)
    private val ois = ObjectInputStream(inputStream)

    suspend fun readObject(): Any {
        // pretty hacky, I don't think an asyncobjectinputstream exists :(
        return withContext(Dispatchers.IO) {
            @Suppress("BlockingMethodInNonBlockingContext")
            ois.readObject()
        }
    }
}