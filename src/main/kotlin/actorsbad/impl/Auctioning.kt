package actorsbad.impl

import kotlinx.coroutines.channels.ticker
import kotlinx.coroutines.delay
import kotlinx.datetime.Clock
import kotlinx.datetime.Instant
import actorsbad.system.Process
import actorsbad.system.raft.Data
import actorsbad.system.raft.Raft
import actorsbad.system.utils.SInstant
import java.io.Serializable

sealed class AuctioningCommand : Serializable {
    class CreateUser(val pubKey: String) : AuctioningCommand()

    class CreateAuction(
            val ownerID: Long,
            val title: String,
            val description: String,
            val reserve: Int,
            closesAt: Instant,
    ) : AuctioningCommand() {
        private val closesAtS = SInstant(closesAt.toEpochMilliseconds())
        val closesAt
            get() = this.closesAtS.inner
    }

    class CloseAuction(val id: Long, time: Instant) : AuctioningCommand() {
        private val timeS = SInstant(time.toEpochMilliseconds())
        val time
            get() = this.timeS.inner
    }

    class PlaceBid(
            val asUserID: Long,
            val auctionID: Long,
            val amount: Int,
            time: Instant,
    ) : AuctioningCommand() {
        private val timeS = SInstant(time.toEpochMilliseconds())
        val time
            get() = this.timeS.inner
    }

    class UpdateAuctions(time: Instant) : AuctioningCommand() {
        private val timeS = SInstant(time.toEpochMilliseconds())
        val time
            get() = this.timeS.inner
    }
}

sealed class AuctioningQuery : Serializable {
    object GetAuctionList : AuctioningQuery()
    class GetAuction(val id: Long) : AuctioningQuery()
    class FetchUserInfo(val pubKey: String) : AuctioningQuery()
}

@kotlinx.serialization.Serializable
sealed class AuctioningNotification: Serializable {
    @kotlinx.serialization.Serializable
    class AuctionCompleted(val info: AuctionInfo,
                           val didWin: Boolean,
                           val didOwn: Boolean) : AuctioningNotification()
}

@kotlinx.serialization.Serializable
sealed class AuctioningResponse : Serializable {
    @kotlinx.serialization.Serializable
    data class UserInfo(val id: Long) : AuctioningResponse()
    @kotlinx.serialization.Serializable
    data class AuctionCreated(val id: Long) : AuctioningResponse()
    @kotlinx.serialization.Serializable
    data class AuctionClosed(val id: Long) : AuctioningResponse()
    @kotlinx.serialization.Serializable
    object BidPlaced : AuctioningResponse()
    @kotlinx.serialization.Serializable
    data class AuctionList(val auctions: Array<AuctionInfo>) : AuctioningResponse()
    @kotlinx.serialization.Serializable
    data class FoundAuction(val auction: AuctionInfo) : AuctioningResponse()
    @kotlinx.serialization.Serializable
    object Ok : AuctioningResponse()
    @kotlinx.serialization.Serializable
    sealed class Error : AuctioningResponse() {
        @kotlinx.serialization.Serializable
        object NotRegistered : Error()
        @kotlinx.serialization.Serializable
        object UserAlreadyExists : Error()
        @kotlinx.serialization.Serializable
        object AuctionDoesntExist : Error()
        @kotlinx.serialization.Serializable
        object DontOwnAuction : Error()
        @kotlinx.serialization.Serializable
        class CannotBid(val reason: CannotBidReason) : Error()
        @kotlinx.serialization.Serializable
        class CannotCreateAuction(val reason: List<Pair<String, String>>) : Error()
    }
}

class AuctionUpdatePinger(val target: Raft<AuctioningCommand, AuctioningQuery, AuctioningResponse, Auctioning>) : Process() {
    override suspend fun mainFn() {
        delay(5000)
        while (true) {
            delay(1000)
            if (this.target.isLeader()) {
                this.target.invokeCommand(AuctioningCommand.UpdateAuctions(Clock.System.now()))
            }
        }
    }
}

class Auctioning() : Data<AuctioningCommand, AuctioningQuery, AuctioningResponse> {
    private var userIDCounter = 0L
    private var auctionIDCounter = 0L
    private val users: HashMap<Long, User> = HashMap()
    private val usersByPubKey: HashMap<String, User> = HashMap()
    private val auctions: HashMap<Long, Auction> = HashMap()

    override fun command(command: AuctioningCommand): AuctioningResponse {
        return when (command) {
            is AuctioningCommand.CreateUser -> {
                val id = this.userIDCounter++
                val user = User(id, command.pubKey)
                this.users[id] = user
                this.usersByPubKey[command.pubKey] = user
                AuctioningResponse.UserInfo(id)
            }

            is AuctioningCommand.CreateAuction -> {
                val id = this.auctionIDCounter++
                val auction = Auction(
                        command.ownerID, id, command.title, command.description,
                        command.reserve, command.closesAt
                )
                this.auctions[id] = auction
                AuctioningResponse.AuctionCreated(id)
            }

            is AuctioningCommand.CloseAuction -> {
                val auction = this.auctions[command.id]
                        ?: return AuctioningResponse.Error.AuctionDoesntExist
                auction.closedAt = command.time
                AuctioningResponse.AuctionClosed(command.id)
            }

            is AuctioningCommand.PlaceBid -> {
                val auction = this.auctions[command.auctionID]
                        ?: return AuctioningResponse.Error.AuctionDoesntExist
                val bid = Bid(command.asUserID, command.amount, command.time)

                auction.addBid(bid)?.let {
                    AuctioningResponse.Error.CannotBid(it)
                } ?: AuctioningResponse.BidPlaced
            }

            is AuctioningCommand.UpdateAuctions -> {
                for (auction in this.auctions) {
                    if (!auction.value.isClosed(command.time) || auction.value.closedAt != null)
                        continue

                    auction.value.closedAt = command.time
                    val winningBid = auction.value.currentMaxBid()
                    val toNotify = auction.value.bidders() + setOf(auction.value.ownerID)
                    val info = auction.value.asInfo()
                    toNotify.forEach {
                        val didOwn = it == auction.value.ownerID
                        val didWin = it == winningBid?.userID

                        val msg = AuctioningNotification.AuctionCompleted(
                                info, didWin, didOwn
                        )

                        UserSessionManger.notifyUser(it, msg)
                    }
                }

                return AuctioningResponse.Ok
            }
        }
    }

    override fun query(query: AuctioningQuery): AuctioningResponse {
        return when (query) {
            is AuctioningQuery.GetAuctionList -> {
                val infos = this.auctions.values.map {
                    it.asInfo()
                }.toTypedArray()
                AuctioningResponse.AuctionList(infos)
            }
            is AuctioningQuery.GetAuction -> {
                val auction = this.auctions[query.id]
                        ?: return AuctioningResponse.Error.AuctionDoesntExist
                AuctioningResponse.FoundAuction(auction.asInfo())
            }
            is AuctioningQuery.FetchUserInfo -> {
                val user = this.usersByPubKey[query.pubKey] ?: return AuctioningResponse.Error.NotRegistered
                AuctioningResponse.UserInfo(user.id)
            }
        }
    }
}