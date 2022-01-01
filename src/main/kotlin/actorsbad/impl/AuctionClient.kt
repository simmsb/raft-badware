package actorsbad.impl

import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.datetime.Clock
import kotlinx.datetime.Instant
import actorsbad.impl.communication.AsyncEncryptedTransportClient
import actorsbad.impl.communication.Commands
import actorsbad.impl.communication.MessagedTransport
import actorsbad.impl.discovery.RedisDiscovery
import actorsbad.system.socket.tcpConnect
import actorsbad.system.utils.AsyncSocketChannel
import actorsbad.system.utils.Result
import java.io.IOException
import java.net.ConnectException
import kotlin.coroutines.coroutineContext
import kotlin.time.ExperimentalTime
import kotlin.time.seconds

class AuctionClient(
        val discovery: RedisDiscovery,
        var socket: AsyncEncryptedTransportClient,
        val ourPubKey: ByteArray,
        val ourPrivKey: ByteArray,
        val serverPubKey: ByteArray,
) {
    // Ideally we'd just put the socket in a process that handles reconnecting, but w/e
    private val reconnectMutex = Mutex()

    suspend fun notifLoop() {
        while (true) {
            when (val n = this.readNotificationReconnecting()) {
                is AuctioningNotification.AuctionCompleted -> {
                    when {
                        n.didOwn -> {
                            println("An auction you opened completed: ${n.info.title}, #${n.info.id}")

                            when (val w = n.info.currentMaxBid) {
                                null -> println("It closed with no bidders")
                                else -> println("It closed with a winning bid of $w")
                            }
                        }
                        n.didWin -> {
                            println("An auction you bidded in completed: ${n.info.title}, #${n.info.id}")
                            println("Congratulations, you won this auction with a bid of ${n.info.currentMaxBid}")
                        }
                        else -> {
                            println("An auction you bidded in completed: ${n.info.title}, #${n.info.id}")
                            println("You did not win the auction.")
                        }
                    }
                }
            }
        }
    }

    private suspend fun readLineA(): String {
        return withContext(Dispatchers.IO) {
            readLine()!!
        }
    }

    suspend fun commandLoop() {
        while (true) {
            print("> ")
            val inp = this.readLineA().split(" ")
            val cmd = inp.firstOrNull()
            val params = inp.drop(1)

            when (cmd) {
                "register" -> this.registerCmd()
                "create" -> this.createAuctionCmd()
                "close" -> {
                    val id = params.firstOrNull()?.toLongOrNull()

                    if (id == null) {
                        println("format: close <auction id>")
                        continue
                    }

                    this.closeAuctionCmd(id)
                }
                "bid" -> {
                    val id = params.getOrNull(0) ?.toLongOrNull()
                    val amount = params.getOrNull(1) ?.toIntOrNull()

                    if (id == null || amount == null) {
                        println("format: bid <auction id> <amount>")
                        continue
                    }

                    this.placeBidCmd(id, amount)
                }
                "list" -> {
                    val showClosed = params.firstOrNull() ?.let { it == "closed" } ?: false
                    this.listAuctionsCmd(showClosed)
                }
                "view" -> {
                    val id = params.firstOrNull()?.toLongOrNull()

                    if (id == null) {
                        println("format: view <auction id>")
                        continue
                    }

                    this.viewAuctionCmd(id)
                }

                else -> {
                    println("""
                        commands:
                          register - register an account
                          create - create an auction (interactive command)
                          close <id> - close an auction (that you own) early
                          bid <id> <amount> - bid in an auction
                          list [closed] - list auctions (pass `closed` to view closed)
                          view <id> - view an auction
                    """.trimIndent())
                }
            }
        }
    }

    private suspend fun registerCmd() {
        when (val r = this.register()) {
            is Result.Ok ->
                println("Registered as a user with ID: ${r.ok.id}")

            is Result.Err ->
                println("Failed to register")
        }
    }

    @OptIn(ExperimentalTime::class)
    private suspend fun createAuctionCmd() {
        print("title: ")
        val title = this.readLineA()

        print("description: ")
        val description = this.readLineA()

        print("reserve: ")
        val reserve = this.readLineA().toIntOrNull()
        if (reserve == null) {
            println("Invalid reserve price!")
            return
        }

        print("close in (seconds): ")
        val seconds = this.readLineA().toIntOrNull()
        if (seconds == null) {
            println("Invalid auction duration!")
            return
        }

        val closesAt = Clock.System.now() + seconds.seconds

        when (val r = this.createAuction(title, description, reserve, closesAt)) {
            is Result.Ok ->
                println("Auction created with ID: #${r.ok.id}")

            is Result.Err -> when (r.err) {
                is AuctioningResponse.Error.CannotCreateAuction -> {
                    println("Couldn't create the auction for the following reasons:")
                    for (err in r.err.reason) {
                        println("field ${err.first}: ${err.second}")
                    }
                }

                is AuctioningResponse.Error.NotRegistered ->
                    println("You must be registered to create auctions")
            }
        }
    }

    private suspend fun closeAuctionCmd(auctionID: Long) {
        when (val r = this.closeAuction(auctionID)) {
            is Result.Ok ->
                println("Auction closes successfully!")

            is Result.Err -> when (r.err) {
                is AuctioningResponse.Error.AuctionDoesntExist ->
                    println("That auction doesn't exist.")

                is AuctioningResponse.Error.DontOwnAuction ->
                    println("You don't own that auction")

                else ->
                    println("Closing that auction failed")
            }
        }
    }

    private suspend fun placeBidCmd(auctionID: Long, amount: Int) {
        when (val r = this.placeBid(auctionID, amount)) {
            is Result.Ok ->
                println("Bid placed successfully!")

            is Result.Err -> when (r.err) {
                is AuctioningResponse.Error.CannotBid ->
                    println("Bid failed: ${r.err.reason}")

                is AuctioningResponse.Error.NotRegistered ->
                    println("You must be registered to place bids")

                else ->
                    println("Unknown error occurred when placing bids")
            }
        }
    }

    private suspend fun listAuctionsCmd(showClosed: Boolean) {
        when (val r = this.getAuctionList()) {
            is Result.Ok -> {
                val now = Clock.System.now()

                for (auction in r.ok) {
                    if (auction.isClosed(now) != showClosed)
                        continue

                    val closedInfo = if (auction.isClosed(now)) {
                        "[CLOSED]"
                    } else {
                        "in ${auction.closesAt - now}"
                    }

                    println("""
                        Auction: ${auction.title} #${auction.id}
                            Current bid: ${auction.currentMaxBid ?: "none"}
                            Closes: $closedInfo
                    """.trimIndent())
                }
            }

            is Result.Err -> {
                println("Failed to list actions: ${r.err}")
            }
        }
    }

    @OptIn(ExperimentalTime::class)
    private suspend fun viewAuctionCmd(auctionID: Long) {
        when (val r = this.getAuction(auctionID)) {
            is Result.Ok -> {
                val now = Clock.System.now()
                val closedInfo = if (r.ok.isClosed(now)) {
                    "[CLOSED]"
                } else {
                    "in ${r.ok.closesAt - now}"
                }

                println("""
                    Auction: ${r.ok.title} #${auctionID}
                    Description: ${r.ok.description}
                    Reserve: ${r.ok.reserve}
                    Current top bid: ${r.ok.currentMaxBid ?: "none"}
                    Closes: $closedInfo
                """.trimIndent())
            }

            is Result.Err ->
                println("This auction doesn't exist")
        }
    }

    suspend fun register(): Result<AuctioningResponse.UserInfo, AuctioningResponse.Error> {
        return when (val r = this.runCommandReconnecting(Commands.CreateUser)) {
            is AuctioningResponse.UserInfo ->
                Result.Ok(r)

            is AuctioningResponse.Error ->
                Result.Err(r)

            else ->
                throw Exception("unexpected error when creating user: $r")
        }
    }

    suspend fun createAuction(title: String, desc: String, reserve: Int, closesAt: Instant):
            Result<AuctioningResponse.AuctionCreated, AuctioningResponse.Error> {
        val cmd = Commands.CreateAuction(title, desc, reserve, closesAt.toEpochMilliseconds())
        return when (val r = this.runCommandReconnecting(cmd)) {
            is AuctioningResponse.AuctionCreated ->
                Result.Ok(r)

            is AuctioningResponse.Error ->
                Result.Err(r)

            else ->
                throw Exception("unexpected error when creating auction: $r")
        }
    }

    suspend fun closeAuction(auctionID: Long): Result<AuctioningResponse.AuctionClosed, AuctioningResponse.Error> {
        return when (val r = this.runCommandReconnecting(Commands.CloseAuction(auctionID))) {
            is AuctioningResponse.AuctionClosed ->
                Result.Ok(r)

            is AuctioningResponse.Error ->
                Result.Err(r)

            else ->
                throw Exception("unexpected error when closing auction: $r")
        }
    }

    suspend fun placeBid(auctionID: Long, amount: Int): Result<AuctioningResponse.BidPlaced, AuctioningResponse.Error> {
        return when (val r = this.runCommandReconnecting(Commands.PlaceBid(auctionID, amount))) {
            is AuctioningResponse.BidPlaced ->
                Result.Ok(r)

            is AuctioningResponse.Error ->
                Result.Err(r)

            else ->
                throw Exception("unexpected error when bidding: $r")
        }
    }

    suspend fun getAuctionList(): Result<Array<AuctionInfo>, AuctioningResponse.Error> {
        return when (val r = this.runCommandReconnecting(Commands.GetAuctionList)) {
            is AuctioningResponse.AuctionList ->
                Result.Ok(r.auctions)

            is AuctioningResponse.Error ->
                Result.Err(r)

            else ->
                throw Exception("unexpected error when getting auction list: $r")
        }
    }

    suspend fun getAuction(auctionID: Long): Result<AuctionInfo, AuctioningResponse.Error> {
        return when (val r = this.runCommandReconnecting(Commands.GetAuction(auctionID))) {
            is AuctioningResponse.FoundAuction ->
                Result.Ok(r.auction)

            is AuctioningResponse.Error ->
                Result.Err(r)

            else ->
                throw Exception("unexpected error when getting auction list: $r")
        }
    }

    private suspend fun runCommandReconnecting(cmd: Commands): AuctioningResponse {
        outer@ while (true) {
            try {
                this.socket.sendMessage(cmd)
                return this.socket.receiveResponse()
            } catch (e: IOException) {
                this.reconnect()
            }
        }
    }

    private suspend fun readNotificationReconnecting(): AuctioningNotification {
        outer@ while (true) {
            try {
                return this.socket.receiveNotification()
            } catch (e: IOException) {
                this.reconnect()
            }
        }
    }

    private suspend fun reconnect() {
        if (this.reconnectMutex.isLocked) {
            // wait for it to unlock and retry
            this.reconnectMutex.withLock {  }
        } else {
            this.reconnectMutex.withLock {
                val retries = 5
                val endpoints = this.discovery.getServers()
                this.socket = createSocketRetrying(retries, endpoints, this.ourPubKey, this.ourPrivKey, this.serverPubKey) ?:
                        throw Exception("Couldn't reconnect to server after $retries retries!")
            }
        }
    }

    companion object {
        private suspend fun createSocketRetrying(
                retries: Int,
                endpoints: List<Pair<String, Int>>,
                ourPubKey: ByteArray,
                ourPrivKey: ByteArray,
                serverPubKey: ByteArray
        ): AsyncEncryptedTransportClient? {
            for (i in 0..retries) {
                val s = createSocket(endpoints, ourPubKey, ourPrivKey, serverPubKey)
                if (s != null) return s
                delay(100)
            }
            return null
        }

        private suspend fun createSocket(endpoints: List<Pair<String, Int>>,
                                         ourPubKey: ByteArray,
                                         ourPrivKey: ByteArray,
                                         serverPubKey: ByteArray): AsyncEncryptedTransportClient? {
            val (host, port) = endpoints.random()

            val s = try {
                tcpConnect(host, port)
            } catch (e: ConnectException) {
                return null
            }

            val channel = AsyncEncryptedTransportClient(MessagedTransport(AsyncSocketChannel(s)),
                    ourPrivKey, ourPubKey, serverPubKey
            )

            if (channel.initiate()) {
                CoroutineScope(coroutineContext).launch {
                    channel.runReceiver()
                }
            } else {
                return null
            }

            return channel
        }

        suspend fun connect(endpoints: List<Pair<String, Int>>, discovery: RedisDiscovery, ourPubKey: ByteArray, ourPrivKey: ByteArray, serverPubKey: ByteArray): AuctionClient? {
            val channel = this.createSocket(endpoints, ourPubKey, ourPrivKey, serverPubKey) ?: return null
            return AuctionClient(discovery, channel, ourPubKey, ourPrivKey, serverPubKey)
        }
    }
}