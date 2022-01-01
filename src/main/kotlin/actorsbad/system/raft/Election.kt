package actorsbad.system.raft

import kotlinx.datetime.Clock
import kotlinx.datetime.Instant
import actorsbad.system.PID
import java.util.concurrent.TimeUnit
import kotlin.random.Random
import kotlin.time.ExperimentalTime
import kotlin.time.toDuration

class Election(votedFor: PID? = null,
               private var votes: HashSet<PID>? = null,
               var timer: Timer? = null,
               var lastLeaderMessage: Instant? = null
) {
    var votedFor = votedFor
        private set

    suspend fun updateForCandidate(config: Config) {
        this.timer?.cancel()
        val thisPID = Server.getPID()
        this.votes = hashSetOf(thisPID)
        this.votedFor = thisPID
        this.timer = createElectionTimer(thisPID, config)
        this.timer?.start()
    }

    suspend fun updateForFollower(config: Config) {
        this.timer?.cancel()
        this.votedFor = null
        this.votes = null
        val thisPID = Server.getPID()
        this.timer = createElectionTimer(thisPID, config)
        this.timer?.start()
    }

    suspend fun voteFor(candidate: PID, config: Config) {
        this.timer?.cancel()
        this.votedFor = candidate
        val thisPID = Server.getPID()
        this.timer = createElectionTimer(thisPID, config)
        this.timer?.start()
    }

    /**
     * @return if we won the election
     */
    suspend fun gainVote(members: Members, voter: PID): Boolean {
        this.votes!!.add(voter)
        val doWeHaveMajority = this.votes!!.size >= (members.allMembers().size / 2) + 1
        if (doWeHaveMajority) {
            this.timer?.cancel()
        }
        return doWeHaveMajority
    }

    suspend fun resetTimer(config: Config) {
        this.timer?.cancel()
        val thisPID = Server.getPID()
        this.timer = createElectionTimer(thisPID, config)
        this.timer?.start()
        this.lastLeaderMessage = this.timer!!.startedAt
    }

    fun shouldInitiateElection(config: Config): Boolean {
        val lastLeaderMessage = this.lastLeaderMessage ?: return true
        return lastLeaderMessage + config.electionTimeout <= Clock.System.now()
    }

    companion object {
        suspend fun forLeader(): Election {
            return Election(Server.getPID())
        }

        suspend fun forFollower(config: Config): Election {
            val thisPID = Server.getPID()
            val timer = this.createElectionTimer(thisPID, config)
            timer.start()
            return Election(thisPID, timer = timer, lastLeaderMessage = timer.startedAt)
        }

        @OptIn(ExperimentalTime::class)
        fun createElectionTimer(thisPID: PID, config: Config): Timer {
            val offset = Random.nextLong(config.electionTimeout.toLongMilliseconds()).toDuration(TimeUnit.MILLISECONDS)
            val timeout = config.electionTimeout + offset
            return Timer(timeout, Timeouts.ElectionTimeout, thisPID)
        }
    }
}
