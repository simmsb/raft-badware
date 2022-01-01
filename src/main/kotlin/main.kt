import io.lettuce.core.RedisClient
import io.lettuce.core.RedisURI
import kotlinx.coroutines.*
import actorsbad.impl.*
import actorsbad.impl.communication.Crypto
import actorsbad.impl.communication.UserFacingSocketServer
import actorsbad.impl.discovery.RedisDiscovery
import actorsbad.system.Pinger
import actorsbad.system.Process
import actorsbad.system.Registry
import actorsbad.system.raft.AddFollowerException
import actorsbad.system.raft.Raft
import actorsbad.system.raft.RaftError
import actorsbad.system.socket.SocketNode
import actorsbad.system.socket.SocketServer
import sun.misc.Signal
import java.net.ConnectException
import java.net.InetAddress
import java.security.KeyPairGenerator
import java.security.SecureRandom
import kotlin.coroutines.coroutineContext
import kotlin.random.Random
import kotlin.system.exitProcess

fun createRedis(): RedisClient {
    val host = System.getenv("REDIS_HOST")
    val port = System.getenv("REDIS_PORT").toInt()

    return RedisClient.create(RedisURI.create(host, port))
}

private suspend fun connectToRemoteNode(it: Pair<String, Int>): Pair<String, Long>? {
    for (i in 0..4) {
        try {
            Registry.getRemoteIdByAddress(it) ?.let { id ->
                return Pair("auction${it.first}", id)
            }

            val remote = Registry.connectToRemoteNodes(listOf(it)) { scope, (host, port) ->
                SocketNode.connect(scope, host, port)
            }.firstOrNull() ?: continue

            val id = remote.id() ?: throw Exception("Remote had no ID?")
            return Pair("auction${it.first}", id)
        } catch (e: ConnectException) {
            println("Initial connection failed for $it, $e")
            delay(100)
        }
    }

    return null
}

class DieOnRegistryDown : Process() {
    override suspend fun mainFn() {
        try {
            this.receive()
        } catch (e: CancellationException) {
        } finally {
            exitProcess(0)
        }
    }
}

fun main(args: Array<String>) {
    println("hi")
    when {
        args.lastOrNull() == "server" -> {
            runBlocking {
                coroutineScope {
                    val hostname = InetAddress.getLocalHost().hostAddress

                    Registry.init(this)
                    val dieWhenDead = Registry.spawn(DieOnRegistryDown())
                    Registry.spawn("pinger", Pinger())
                    Registry.spawn(SocketServer(hostname, System.getenv("BACKEND_PORT")!!.toInt()))
                    UserSessionManger.spawn()

                    // delay startup
                    delay(Random.nextLong(10, 100))

                    val discovery = RedisDiscovery(createRedis())
                    val existing = discovery.getAndPublish()
                    println("existing $existing")

                    val raft = if (existing.isEmpty()) {
                        println("Node $hostname creating a new group")
                        Raft.newGroup(Raft.baseConfig(), "auction$hostname") {
                            Auctioning()
                        }
                    } else {
                        println("Node $hostname joining an existing group")
                        val remotes = arrayListOf<Pair<String, Long>>()
                        for (remote in existing) {
                            connectToRemoteNode(remote) ?.let { remotes.add(it) }
                        }

                        if (remotes.isEmpty()) {
                            throw Exception("Failed to connect to a member of the group")
                        }

                        println("Connecting to: $remotes")

                        // kotlin is bad
                        lateinit var r: Raft<AuctioningCommand, AuctioningQuery, AuctioningResponse, Auctioning>
                        try {
                            r = Raft.joinGroup(remotes, "auction$hostname")
                        } catch (e: AddFollowerException) {
                            exitProcess(0)
                        }
                        r
                    }

                    Signal.handle(Signal("INT")) {
                        stopOnSignal(raft)
                    }

                    Signal.handle(Signal("TERM")) {
                        stopOnSignal(raft)
                    }

                    Registry.spawn(AuctionUpdatePinger(raft))
                    Registry.spawn(UserFacingSocketServer(hostname,
                            System.getenv("PORT")!!.toInt(),
                            Crypto.privateKey, Crypto.publicKey, raft))

                    launch {
                        discovery.publishLoop()
                    }
                }
            }
        }

        else -> {
            runBlocking {
                val keypairgen = KeyPairGenerator.getInstance("RSA")
                keypairgen.initialize(2048, SecureRandom())
                val pair = keypairgen.genKeyPair()

                val discovery = RedisDiscovery(createRedis())
                lateinit var endpoints: List<Pair<String, Int>>

                for (i in 0..5) {
                    endpoints = discovery.getServers()
                    println("endpoints: $endpoints")
                    if (endpoints.isNotEmpty())
                        break

                    delay(100)
                }

                if (endpoints.isEmpty())
                    throw Exception("Couldn't find any endpoints after 5 attempts")

                val client = AuctionClient.connect(endpoints, discovery,
                        pair.public.encoded, pair.private.encoded,
                        Crypto.publicKey) ?: throw Exception("Couldn't connect to server?")
                this.launch {
                    client.notifLoop()
                }

                client.commandLoop()
            }
        }
    }
}

private fun CoroutineScope.stopOnSignal(raft: Raft<AuctioningCommand, AuctioningQuery, AuctioningResponse, Auctioning>) {
    launch {
        Registry.spawn(object : Process() {
            override suspend fun mainFn() {
                print("leaving group")
                try {
                    raft.leaveGroup()
                } catch (e: Exception) {
                    e.printStackTrace()
                    println("Leaving failed with: $e")
                } finally {
                    exitProcess(0)
                }
            }
        })

        delay(1000)
        exitProcess(0)
    }
}

