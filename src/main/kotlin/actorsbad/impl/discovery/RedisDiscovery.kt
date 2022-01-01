package actorsbad.impl.discovery

import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.lettuce.core.RedisClient
import io.lettuce.core.api.coroutines
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.toList
import java.net.InetAddress

class RedisDiscovery(redisClient: RedisClient) {
    private val connection = redisClient.connect()

    @OptIn(ExperimentalLettuceCoroutinesApi::class)
    private val commands = this.connection.coroutines()

    private val hostName: String by lazy { InetAddress.getLocalHost().hostAddress }

    private val port: Int by lazy {
        System.getenv("PORT")?.toIntOrNull() ?: throw Exception("PORT was not set or invalid")
    }

    private val backendPort: Int by lazy {
        System.getenv("BACKEND_PORT")?.toIntOrNull() ?: throw Exception("BACKEND_PORT was not set or invalid")
    }

    private val connString
        get() = "${this.hostName}:${this.port}:${this.backendPort}"

    @OptIn(ExperimentalLettuceCoroutinesApi::class)
    suspend fun publish() {
        this.commands.psetex("server:${this.connString}", 2500, this.connString)
    }

    suspend fun publishLoop() {
        while (true) {
            this.publish()
            delay(1500)
        }
    }

    private fun parseServers(l: List<String>): List<Triple<String, Int, Int>>
        = l.map {
        val parts = it.split(":")

        val hostname = parts.getOrNull(1) ?: throw Exception("Service had no hostname")
        val port = parts.getOrNull(2)?.toInt() ?: throw Exception("Service had no port")
        val backendPort = parts.getOrNull(3)?.toInt() ?: throw Exception("Service had no port")

        Triple(hostname, port, backendPort)
    }

    @OptIn(ExperimentalLettuceCoroutinesApi::class)
    suspend fun getAndPublish(): List<Pair<String, Int>> {
        val servers = this.getBackendServers()
        this.publish()
        return servers

        // SET inside transactions is broken apparently? just hope two people don't publish at the same time
//        for (i in 0..5) {
//            println("publish attempt")
//            val r: TransactionResult = this.commands.multi {
//                this.keys("server:*")
//                this.psetex("server:${this@RedisDiscovery.connString}", 1500, this@RedisDiscovery.connString)
//            } ?: continue
//
//            println("r: $r")
//
//            if (r.wasDiscarded())
//                continue
//
//            return this.parseServers(r.get(1))
//        }
//
//        throw Exception("Tried to get and publish five times with failure")
    }

    @OptIn(ExperimentalLettuceCoroutinesApi::class)
    suspend fun getServers(): List<Pair<String, Int>> {
        return this.parseServers(this.commands.keys("server:*").toList())
                .map { Pair(it.first, it.second) }
    }

    @OptIn(ExperimentalLettuceCoroutinesApi::class)
    suspend fun getBackendServers(): List<Pair<String, Int>> {
        return this.parseServers(this.commands.keys("server:*").toList())
                .map { Pair(it.first, it.third) }
    }
}