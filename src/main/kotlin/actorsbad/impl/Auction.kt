package actorsbad.impl

import kotlinx.datetime.Instant
import actorsbad.system.utils.SInstant
import java.io.Serializable

class Bid(val userID: Long, val amount: Int, time: Instant) : Serializable {
    private val timeS = SInstant(time.toEpochMilliseconds())
    val time
        get() = this.timeS.inner
}

@kotlinx.serialization.Serializable
sealed class CannotBidReason(val reason: String) : Serializable {
    @kotlinx.serialization.Serializable
    object BidderIsOwner : CannotBidReason("Bidder is the owner of the auction")

    @kotlinx.serialization.Serializable
    object BiddedTooLow : CannotBidReason("The bid is lower than the reserve or current top bid")

    @kotlinx.serialization.Serializable
    object AuctionClosed : CannotBidReason("This auction closed before the bid was placed")
}

class Auction(
        val ownerID: Long,
        val id: Long,
        val title: String,
        val description: String,
        val reserve: Int,
        closesAt: Instant
) : Serializable {
    private var closedAtS: SInstant? = null
    var closedAt
        get() = this.closedAtS?.inner
        set(value) {
            this.closedAtS = value?.let { SInstant(it.toEpochMilliseconds()) }
        }

    private val closesAtS = SInstant(closesAt.toEpochMilliseconds())
    val closesAt
        get() = this.closesAtS.inner

    val bids: ArrayList<Bid> = ArrayList()

    private fun canBid(bid: Bid): CannotBidReason? {
        if (bid.userID == this.ownerID)
            return CannotBidReason.BidderIsOwner

        if (this.isClosed(bid.time))
            return CannotBidReason.AuctionClosed

        val maxBid = this.currentMaxBid()?.amount ?: 0
        if (bid.amount < maxBid || bid.amount < this.reserve)
            return CannotBidReason.BiddedTooLow

        return null
    }

    /**
     * Apply a bid, returns null on success, CannotBidReason on failure
     */
    fun addBid(bid: Bid): CannotBidReason? {
        val r = this.canBid(bid)
        if (r == null)
            this.bids.add(bid)
        return r
    }

    fun currentMaxBid(): Bid? {
        return this.bids.maxByOrNull { it.amount }
    }

    fun closeTime(): Instant {
        return this.closedAt?.let { minOf(it, this.closesAt) } ?: this.closesAt
    }

    fun isClosed(now: Instant): Boolean {
        return this.closeTime() < now
    }

    fun asInfo(): AuctionInfo =
            AuctionInfo.new(this.ownerID, this.id, this.title, this.description,
                    this.reserve, this.currentMaxBid()?.amount, this.closedAt, this.closesAt)

    fun bidders(): Set<Long> {
        return this.bids.map { it.userID }.toSet()
    }

    companion object {
        fun checkIfValid(
                title: String,
                description: String,
                reserve: Int,
                closesAt: Instant,
                now: Instant
        ): List<Pair<String, String>> {
            val errors: ArrayList<Pair<String, String>> = ArrayList()

            errors.addErrIf("title", "Length must be at least 3 characters") {
                title.length < 3
            }

            errors.addErrIf("title", "Length must be at most 20 characters") {
                title.length > 20
            }

            errors.addErrIf("description", "Length must be at least 3 characters") {
                description.length < 3
            }

            errors.addErrIf("description", "Length must be at most 100 characters") {
                description.length > 100
            }

            errors.addErrIf("reserve", "Reserve must be > 0") {
                reserve < 0
            }

            errors.addErrIf("closesAt", "Close date is in the past") {
                closesAt < now
            }

            return errors
        }
    }
}

private fun ArrayList<Pair<String, String>>.addErrIf(name: String, error: String, cb: () -> Boolean) {
    if (cb())
        this.add(Pair(name, error))
}

@kotlinx.serialization.Serializable
class AuctionInfo(
        val ownerID: Long,
        val id: Long,
        val title: String,
        val description: String,
        val reserve: Int,
        val currentMaxBid: Int?,
        val closedAtS: SInstant?,
        val closesAtS: SInstant
) : Serializable {
    val closedAt
        get() = this.closedAtS?.inner

    val closesAt
        get() = this.closesAtS.inner

    fun closeTime(): Instant {
        return this.closedAt ?.let { minOf(it, this.closesAt) } ?: this.closesAt
    }

    fun isClosed(now: Instant): Boolean {
        return this.closeTime() < now
    }

    fun canBid(bid: Bid): CannotBidReason? {
        if (bid.userID == this.ownerID)
            return CannotBidReason.BidderIsOwner

        if (this.isClosed(bid.time))
            return CannotBidReason.AuctionClosed

        if (bid.amount < (this.currentMaxBid ?: 0) || bid.amount < this.reserve)
            return CannotBidReason.BiddedTooLow

        return null
    }

    companion object {
        fun new(
                ownerID: Long,
                id: Long,
                title: String,
                description: String,
                reserve: Int,
                currentMaxBid: Int?,
                closedAt: Instant?,
                closesAt: Instant
        ) = AuctionInfo(
                ownerID, id, title, description, reserve, currentMaxBid,
                closedAt ?.let { SInstant(it.toEpochMilliseconds()) },
                SInstant(closesAt.toEpochMilliseconds())
        )
    }
}
