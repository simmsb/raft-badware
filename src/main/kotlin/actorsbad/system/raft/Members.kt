package actorsbad.system.raft

import actorsbad.system.PID
import actorsbad.system.Registry
import java.io.Serializable

class Members(
        leader: PID?,
        private val all: HashSet<PID>,
        private var uncommittedMembershipChange: Triple<LogInfo, PID, Boolean>? = null,
        pendingLeaderChange: PID? = null,
) : Serializable {

    var leader = leader
        private set
    var pendingLeaderChange = pendingLeaderChange
        private set

    fun allMembers(): Set<PID> {
        return this.all.toSet()
    }

    suspend fun otherMembers(): Set<PID> {
        val others = this.all.toMutableSet()
        others.remove(Server.getPID())
        return others
    }

    fun putLeader(leader: PID?) {
        this.leader = leader
        this.pendingLeaderChange = null
    }

    @Throws(MemberManipulationError::class)
    fun addFollower(follower: PID, makeLog: () -> LogEntry): LogEntry {
        throwIf(this.isMembershipChanging()) { MemberManipulationError.MembershipChanging }
        throwIf(this.isLeadershipChanging()) { MemberManipulationError.LeadershipChanging }
        throwIf(this.all.contains(follower)) { MemberManipulationError.AlreadyJoined }

        val logEntry = makeLog()
        this.all.add(follower)
        this.uncommittedMembershipChange = Triple(logEntry.info(), follower, true)
        return logEntry
    }

    @Throws(MemberManipulationError::class)
    suspend fun removeFollower(follower: PID, makeLog: suspend () -> LogEntry): LogEntry {
        val thisPID = Server.getPID()
        throwIf(this.isMembershipChanging()) { MemberManipulationError.MembershipChanging }
        throwIf(this.isLeadershipChanging()) { MemberManipulationError.LeadershipChanging }
        throwIf(follower == thisPID) { MemberManipulationError.WouldRemoveLeader }
        throwIf(!this.all.contains(follower)) { MemberManipulationError.NotAMember }

        println("removing follower: $follower")

        val logEntry = makeLog()
        this.all.remove(follower)
        this.uncommittedMembershipChange = Triple(logEntry.info(), follower, false)
        return logEntry
    }

    @Throws(MemberManipulationError::class)
    suspend fun replaceLeader(leader: PID?, cb: suspend () -> Unit) {
        val thisPID = Server.getPID()
        throwIf(this.isMembershipChanging()) { MemberManipulationError.MembershipChanging }

        cb()

        this.pendingLeaderChange = when (leader) {
            null -> null
            thisPID -> throw MemberManipulationError.AlreadyLeader
            in this.all -> leader
            else -> throw MemberManipulationError.NotAMember
        }
    }

    fun commitMembershipChange(logIndex: Long) {
        val unc = this.uncommittedMembershipChange ?: return

        if (unc.first.index <= logIndex) {
            this.uncommittedMembershipChange = null
        }
    }

    fun maybeResetMembershipChange(logEntry: LogEntry) {
        if (this.uncommittedMembershipChange == null) {
            return
        }

        if (this.uncommittedMembershipChange!!.first != logEntry.info()) {
            return
        }

        if (logEntry.value is LogEntry.LogValue.AddFollower) {
            if (!this.uncommittedMembershipChange!!.third) {
                return
            }

            this.uncommittedMembershipChange = null
        } else if (logEntry.value is LogEntry.LogValue.RemoveFollower) {
            if (this.uncommittedMembershipChange!!.third) {
                return
            }

            this.uncommittedMembershipChange = null
        }
    }

    fun applyMissingEntry(logEntry: LogEntry) {
        when (logEntry.value) {
            is LogEntry.LogValue.AddFollower -> {
                this.all.add(logEntry.value.follower)
                this.uncommittedMembershipChange = Triple(logEntry.info(), logEntry.value.follower, true)
            }

            is LogEntry.LogValue.RemoveFollower -> {
                this.all.remove(logEntry.value.follower)
                this.uncommittedMembershipChange = Triple(logEntry.info(), logEntry.value.follower, false)
            }

            is LogEntry.LogValue.LeaderElected -> {
                this.all.clear()
                this.all.add(logEntry.value.leader)
                this.all.addAll(logEntry.value.followers)
            }
            else -> return
        }

    }

    fun forceRemoveMember(member: PID) {
        val queued = this.uncommittedMembershipChange?.second

        if (queued == member)
            this.uncommittedMembershipChange = null

        this.all.remove(member)

        if (this.pendingLeaderChange == member)
            this.pendingLeaderChange = null
    }

    suspend fun removeDeadMembers() {
        val others = this.otherMembers().toList()
        val stillConnected = Registry.remoteNodes()

        println("removing deadies: $stillConnected, $others")
        for (pid in others) {
            if (pid.nodeID !in stillConnected) {
                this.forceRemoveMember(pid)
            }
        }
    }


    sealed class MemberManipulationError(message: String?) : Throwable(message) {
        object MembershipChanging : MemberManipulationError("Membership changing")
        object LeadershipChanging : MemberManipulationError("Leadership changing")
        object AlreadyJoined : MemberManipulationError("Already joined")
        object WouldRemoveLeader : MemberManipulationError("Would remove leader")
        object NotAMember : MemberManipulationError("Wasn't a member")
        object AlreadyLeader : MemberManipulationError("Already a leader")
        object WouldBreakQuorum : MemberManipulationError("Would break quorum")
        object LeaderUnresponsive : MemberManipulationError("New leader is unresponsive")
    }

    private fun isMembershipChanging(): Boolean {
        return this.uncommittedMembershipChange != null
    }

    private fun isLeadershipChanging(): Boolean {
        return this.pendingLeaderChange != null
    }

    companion object {
        suspend fun newForLonelyLeader(): Members {
            val thisPID = Server.getPID()
            return Members(
                    thisPID,
                    hashSetOf(thisPID),
            )
        }
    }
}

private inline fun throwIf(cond: Boolean, msg: () -> Throwable) {
    if (cond) {
        throw msg()
    }
}