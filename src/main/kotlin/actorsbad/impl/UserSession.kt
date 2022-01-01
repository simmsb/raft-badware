package actorsbad.impl

import kotlinx.datetime.Clock
import kotlinx.datetime.Instant
import actorsbad.impl.communication.AsyncEncryptedTransportServer
import actorsbad.impl.communication.Commands
import actorsbad.system.PID
import actorsbad.system.Process
import actorsbad.system.Registry
import actorsbad.system.messages.ProcessExitMessage
import actorsbad.system.raft.Raft

private class SocketReceiver(
        val dest: PID,
        val socket: AsyncEncryptedTransportServer) : Process() {
    override suspend fun mainFn() {
        Registry.linkSingleDirection(this.self(), this.dest)

        while (true) {
            val r = this.socket.receiveMessage()
            r?.let { this.dest.send(r) }
        }
    }
}

class UserSession(
        private val socket: AsyncEncryptedTransportServer,
        private val pubKey: String,
        private val raft: Raft<AuctioningCommand, AuctioningQuery, AuctioningResponse, Auctioning>,
) : Process() {
    var userID: Long? = null
        private set

    private suspend fun getUserID(): Long? {
        if (this.userID != null)
            return this.userID!!

        when (val r = this.raft.invokeQuery(AuctioningQuery.FetchUserInfo(this.pubKey))) {
            is AuctioningResponse.UserInfo ->
                this.userID = r.id

            else ->
                return null
        }

        return this.userID!!
    }

    override suspend fun mainFn() {
        println("user connected")

        Registry.spawn(SocketReceiver(this.self(), this.socket))

        this.getUserID() ?.let {
            UserSessionManger.addMonitor(it)
        }

        while (true) {
            val r = when (val c = this.receive()) {
                is Commands.CreateUser -> {
                    println("creating user")
                    when (this.raft.invokeQuery(AuctioningQuery.FetchUserInfo(this.pubKey))) {
                        is AuctioningResponse.UserInfo ->
                            AuctioningResponse.Error.UserAlreadyExists

                        else -> {
                            val r = this.raft.invokeCommand(AuctioningCommand.CreateUser(this.pubKey))

                            (r as? AuctioningResponse.UserInfo) ?.let {
                                UserSessionManger.addMonitor(it.id)
                            }

                            r
                        }
                    }
                }

                is Commands.CreateAuction -> {
                    val now = Clock.System.now()
                    val userID = this.getUserID()

                    if (userID == null) {
                        AuctioningResponse.Error.NotRegistered
                    } else {
                        val errors = Auction.checkIfValid(
                                c.title, c.description, c.reserve,
                                Instant.fromEpochMilliseconds(c.closesAt),
                                now
                        )

                        if (errors.isNotEmpty()) {
                            AuctioningResponse.Error.CannotCreateAuction(errors)
                        } else {
                            this.raft.invokeCommand(AuctioningCommand.CreateAuction(
                                    userID,
                                    c.title,
                                    c.description,
                                    c.reserve,
                                    Instant.fromEpochMilliseconds(c.closesAt),
                            ))
                        }
                    }
                }
                is Commands.CloseAuction -> {
                    this.withUser { uid ->
                        this.withAuction(c.id) { auction ->
                            if (auction.ownerID == uid) {
                                val now = Clock.System.now()
                                this.raft.invokeCommand(AuctioningCommand.CloseAuction(c.id, now))
                            } else {
                                AuctioningResponse.Error.DontOwnAuction
                            }
                        }
                    }
                }
                is Commands.PlaceBid -> {
                    this.withUser { uid ->
                        this.withAuction(c.auctionID) { auction ->
                            val now = Clock.System.now()
                            val bid = Bid(uid, c.amount, now)
                            val cannotBidReason = auction.canBid(bid)
                            if (cannotBidReason == null) {
                                this.raft.invokeCommand(AuctioningCommand.PlaceBid(uid, c.auctionID, c.amount, now))
                            } else {
                                AuctioningResponse.Error.CannotBid(cannotBidReason)
                            }
                        }
                    }
                }

                is Commands.GetAuctionList ->
                    this.raft.invokeQuery(AuctioningQuery.GetAuctionList)

                is Commands.GetAuction ->
                    this.raft.invokeQuery(AuctioningQuery.GetAuction(c.auctionID))

                is AuctioningNotification -> {
                    this.socket.sendNotif(c)
                    continue
                }

                is ProcessExitMessage ->
                    break

                else -> {
                    println("user session unknown message: $c")
                    continue
                }
            }

            this.socket.sendResponse(r)
        }
        this.socket.close()
    }

    private suspend fun withAuction(id: Long, cb: suspend (AuctionInfo) -> AuctioningResponse): AuctioningResponse {
        return when (val r = this.raft.invokeQuery(AuctioningQuery.GetAuction(id))) {
            is AuctioningResponse.FoundAuction ->
                cb(r.auction)

            else ->
                AuctioningResponse.Error.AuctionDoesntExist
        }
    }

    private suspend fun withUser(cb: suspend (Long) -> AuctioningResponse): AuctioningResponse {
        return when (val r = this.raft.invokeQuery(AuctioningQuery.FetchUserInfo(this.pubKey))) {
            is AuctioningResponse.UserInfo ->
                cb(r.id)

            else ->
                AuctioningResponse.Error.NotRegistered
        }
    }
}