package actorsbad.system.socket

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import actorsbad.system.utils.AsyncObjectInputStream
import actorsbad.system.utils.AsyncObjectOutputStream
import java.io.Serializable

class SocketMultiplexer(scope: CoroutineScope, private val oos: AsyncObjectOutputStream, private val ois: AsyncObjectInputStream) {
    private val initiatorToInitiate = Channel<Any>(Channel.UNLIMITED)
    private val initiateToInitiator = Channel<Any>(Channel.UNLIMITED)
    private val toWrite = Channel<Serializable>(Channel.UNLIMITED)

    init {
        scope.launch {
            try {
                while (true) {
                    val r = this@SocketMultiplexer.ois.readObject()
                    when (r) {
                        is AtoB<*> -> this@SocketMultiplexer.initiateToInitiator.send(r.msg)
                        is BtoA<*> -> this@SocketMultiplexer.initiatorToInitiate.send(r.msg)
                    }
                }
            } catch (e: Exception) {
                this@SocketMultiplexer.initiateToInitiator.close(e)
                this@SocketMultiplexer.initiatorToInitiate.close(e)
            }
        }

        scope.launch {
            try {
                while (true) {
                    oos.writeObject(toWrite.receive())
                }
            } catch (e: Exception) {
                println("Socketmulti died with: $e")
                toWrite.close(e)
            }
        }
    }

    suspend fun <T : Serializable> sendToInitiator(msg: T) {
        this.toWrite.send(AtoB(msg))
    }

    suspend fun <T : Serializable> sendToInitiate(msg: T) {
        this.toWrite.send(BtoA(msg))
    }

    suspend fun receiveFromInitiator(): Any {
        return this.initiatorToInitiate.receive()
    }

    fun fromInitiator(): Flow<Any> = flow {
        this.emitAll(this@SocketMultiplexer.initiatorToInitiate)
    }

    suspend fun receiveFromInitiate(): Any {
        return this.initiateToInitiator.receive()
    }

    fun fromInitiate(): Flow<Any> = flow {
        this.emitAll(this@SocketMultiplexer.initiateToInitiator)
    }

    data class AtoB<T : Serializable>(val msg: T) : Serializable
    data class BtoA<T : Serializable>(val msg: T) : Serializable
}