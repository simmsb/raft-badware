package actorsbad.impl.communication

import kotlinx.serialization.Serializable

@Serializable
sealed class Commands: java.io.Serializable {
    @Serializable
    object CreateUser: Commands()

    @Serializable
    class CreateAuction(val title: String,
                        val description: String,
                        val reserve: Int,
                        val closesAt: Long,
    ): Commands()

    @Serializable
    class CloseAuction(val id: Long): Commands()

    @Serializable
    class PlaceBid(val auctionID: Long, val amount: Int): Commands()

    @Serializable
    object GetAuctionList: Commands()

    @Serializable
    class GetAuction(val auctionID: Long): Commands()
}