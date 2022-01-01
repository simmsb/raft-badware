package actorsbad.system.raft

import java.io.Serializable

sealed class Timeouts: Serializable {
    object ElectionTimeout: Timeouts()
    object HeartbeatTimeout: Timeouts()
    object QuorumTimeout: Timeouts()
}