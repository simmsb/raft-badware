package actorsbad.system.raft

import kotlinx.datetime.Clock
import kotlinx.datetime.Instant
import actorsbad.system.PID
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@OptIn(ExperimentalTime::class)
fun electionTimeoutDelay(config: Config): Duration {
    return config.electionTimeout.times(3)
}

class Leader private constructor(
        private var heartbeatTimer: Timer,
        private var quorumTimer: Timer,
        private val responseTimes: HashMap<PID, Instant>) {
    suspend fun onFollwerResponded(members: Members,
                                   follower: PID,
                                   timestamp: Instant,
                                   config: Config
    ) {
        val followers = members.otherMembers()
        this.responseTimes.merge(follower, timestamp) { a, b -> maxOf(a, b) }
        this.responseTimes.entries.retainAll { it.key in followers }
        val lastReachedQuorum = this.lastReachedQuorum(members) ?: return
        if (this.quorumTimer.startedAt!! < lastReachedQuorum)
            this.resetQuorumTimer(config)
    }

    suspend fun stop() {
        this.heartbeatTimer.cancel()
        this.quorumTimer.cancel()
    }

    suspend fun resetHeartbeatTimer(config: Config) {
        this.heartbeatTimer.cancel()
        this.heartbeatTimer = newHBT(Server.getPID(), config)
        this.heartbeatTimer.start()
    }

    suspend fun resetQuorumTimer(config: Config) {
        this.quorumTimer.cancel()
        this.quorumTimer = newQT(Server.getPID(), config)
        this.quorumTimer.start()
    }

    @OptIn(ExperimentalTime::class)
    suspend fun unresponsiveFollowers(members: Members, config: Config): Collection<PID> {
        val since = Clock.System.now() - electionTimeoutDelay(config)
        return members.otherMembers().filter {
            val lastResponse = this.responseTimes[it] ?: return@filter true
            lastResponse < since
        }
    }

    suspend fun canRemoveFollower(members: Members, follower: PID, config: Config): Boolean {
        val unresponsiveFollowers = this.unresponsiveFollowers(members, config)
        if (follower in unresponsiveFollowers) {
            return true
        }
        val membersAfterRemove = members.allMembers().size - 1
        val healthyMembersAfterRemove = membersAfterRemove - unresponsiveFollowers.size
        return healthyMembersAfterRemove * 2 > membersAfterRemove
    }

    fun removeFollowerResponseTime(follower: PID) {
        this.responseTimes.remove(follower)
    }

    fun hasLeaseExpired(members: Members, config: Config): Boolean {
        val n = members.allMembers().size

        if (n <= 1) {
            return false
        }

        val lastReachedAt = this.lastReachedQuorumInner(n) ?: return true
        return lastReachedAt + config.electionTimeout <= Clock.System.now()
    }

    private fun lastReachedQuorum(members: Members): Instant? {
        val n = members.allMembers().size

        if (n <= 1) {
            return null
        }

        return this.lastReachedQuorumInner(n)
    }

    private fun lastReachedQuorumInner(n: Int): Instant? {
        val half = n / 2
        val responders = this.responseTimes.size

        if (responders < half) {
            return null
        }

        return this.responseTimes.values.sorted().drop(responders - half).firstOrNull()
    }

    companion object {
        private fun newHBT(thisPID: PID, config: Config): Timer {
            return Timer(config.heartbeatTimeout, Timeouts.HeartbeatTimeout, thisPID)
        }

        private fun newQT(thisPID: PID, config: Config): Timer {
            return Timer(electionTimeoutDelay(config), Timeouts.QuorumTimeout, thisPID)
        }

        suspend fun new(config: Config): Leader {
            val thisPID = Server.getPID()
            val hbt = this.newHBT(thisPID, config)
            hbt.start()
            val qt = this.newQT(thisPID, config)
            qt.start()
            return Leader(hbt, qt, HashMap())
        }
    }
}

