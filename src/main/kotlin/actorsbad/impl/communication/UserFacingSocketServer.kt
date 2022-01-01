package actorsbad.impl.communication

import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.collect
import actorsbad.impl.*
import actorsbad.system.Process
import actorsbad.system.Registry
import actorsbad.system.raft.Raft
import actorsbad.system.utils.AsyncSocketChannel
import actorsbad.system.utils.AsyncSocketServer

class UserFacingSocketServer(private val host: String,
                             private val port: Int,
                             private val ourPrivKey: ByteArray,
                             private val ourPubKey: ByteArray,
                             private val raft: Raft<AuctioningCommand, AuctioningQuery, AuctioningResponse, Auctioning>,
) : Process() {
    override suspend fun mainFn() {
        val ss = AsyncSocketServer(host, this.port)

        coroutineScope {
            ss.acceptFlow().collect {
                val encryptedTransport = AsyncEncryptedTransportServer(MessagedTransport(AsyncSocketChannel(it)),
                        this@UserFacingSocketServer.ourPrivKey,
                        this@UserFacingSocketServer.ourPubKey)
                when (val pubKey = encryptedTransport.authenticate()) {
                    null -> {
                        println("Client connected but couldn't auth, closing")
                        @Suppress("BlockingMethodInNonBlockingContext")
                        it.close()
                    }

                    else -> {
                        Registry.spawn(UserSession(encryptedTransport, pubKey,
                                this@UserFacingSocketServer.raft))
                    }
                }
            }
        }
    }
}
