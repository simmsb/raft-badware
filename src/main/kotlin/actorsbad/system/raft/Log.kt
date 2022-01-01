package actorsbad.system.raft

import kotlinx.datetime.Instant
import actorsbad.system.MonitorRef
import actorsbad.system.PID
import java.io.Serializable
import kotlin.math.max

data class LogInfo(val index: Long, val term: Long): Serializable

data class LogEntry(val index: Long, val term: Long, val value: LogValue): Serializable {
    sealed class LogValue: Serializable {
        class Command(val from: PID, val ref: MonitorRef, val argument: Any, val id: Long) : LogValue()
        class Query(val from: PID, val ref: MonitorRef, val argument: Any) : LogValue()
        class ChangeConfig(val config: Config) : LogValue()
        class LeaderElected(val leader: PID, val followers: Array<PID>) : LogValue()
        class AddFollower(val follower: PID) : LogValue()
        class RemoveFollower(val follower: PID) : LogValue()
        class RestoreFromFiles(val pid: PID) : LogValue()
    }

    fun info(): LogInfo = LogInfo(this.index, this.term)
}

private fun <V> entriesBetween(map: Map<Long, V>, min: Long, max: Long): List<V> {
    return (min..max).mapNotNull { map[it] }
}

private fun inverseChangeMembers(logEntry: LogEntry, members: Set<PID>): Set<PID> {
    return when (logEntry.value) {
        is LogEntry.LogValue.AddFollower ->
            members.filter { it != logEntry.value.follower }.toSet()

        is LogEntry.LogValue.RemoveFollower -> {
            val tmpSet = members.toMutableSet()
            tmpSet.add(logEntry.value.follower)
            tmpSet
        }

        else -> members
    }
}

class Logs(private val logMap: HashMap<Long, LogEntry>,
           minIndex: Long,
           maxIndex: Long,
           committedIndex: Long,
        // followersMap only exists on leaders
           private var followerMap: HashMap<PID, Pair<Long, Long>>? = null
) {
    var committedIndex = committedIndex
        private set
    var maxIndex = maxIndex
        private set
    var minIndex = minIndex
        private set

    constructor(lastEntry: LogEntry) : this(
            logMap = hashMapOf(Pair(lastEntry.index, lastEntry)),
            minIndex = lastEntry.index,
            maxIndex = lastEntry.index,
            committedIndex = lastEntry.index,
    )

    // TODO: persistence
    fun commit(): List<LogEntry> {
        val oldCommittedIndex = this.committedIndex
        this.committedIndex = this.maxIndex
        this.cleanOldLogs()
        return entriesBetween(this.logMap, oldCommittedIndex + 1, this.maxIndex)
    }

    @Throws(LogEntryError::class)
    suspend fun constructAppendEntries(term: Long, follower: PID, currentTimestamp: Instant): AsyncMessage.AppendEntriesRequest {
        val followerState = this.followerMap?.get(follower) ?: throw LogEntryError.FollowerDoesntExist

        if (followerState.first < this.minIndex) {
            this.followerMap!![follower] = Pair(this.maxIndex + 1, 0)

            throw LogEntryError.FollowerBehind
        }

        val prevIndex = followerState.first - 1
        val prevTerm = when (val prevEntry = this.logMap[prevIndex]) {
            null -> 0
            else -> prevEntry.term
        }

        return AsyncMessage.AppendEntriesRequest(
                leaderPID = Server.getPID(),
                term = term,
                previousLog = LogInfo(prevIndex, prevTerm),
                entries = entriesBetween(this.logMap, followerState.first, this.maxIndex).toTypedArray(),
                leaderCommittedIndex = this.committedIndex,
                leaderTimestamp = currentTimestamp
        )
    }

    suspend fun setFollowerIndex(members: Members,
                                 currentTerm: Long,
                                 follower: PID,
                                 replicatedIndex: Long): List<LogEntry> {
        this.followerMap!![follower] = Pair(replicatedIndex + 1, replicatedIndex)
        val toKeep = members.otherMembers()
        this.followerMap!!.entries.retainAll {
            it.key in toKeep
        }

        val oldCommittedIndex = this.committedIndex
        this.updateCommitIndex(currentTerm, members)
        return entriesBetween(this.logMap, oldCommittedIndex + 1, this.committedIndex)
    }

    private suspend fun updateCommitIndex(currentTerm: Long, members: Members) {
        val uncommittedLogs = entriesBetween(this.logMap, this.committedIndex + 1, this.maxIndex).reversed()
        val lastCommittableEntry = uncommittedLogs.scan(Triple<LogEntry?, Set<PID>?, Set<PID>>(null, null, members.allMembers()),
                { acc, logEntry ->
                    val forPrev = inverseChangeMembers(logEntry, acc.third)
                    Triple(logEntry, acc.third, forPrev)
                }
        ).drop(1).find {
            this.canCommit(it.first!!, it.second!!, currentTerm)
        } ?: return

        this.committedIndex = lastCommittableEntry.first!!.index
        this.cleanOldLogs()
    }

    private suspend fun canCommit(logEntry: LogEntry, members: Set<PID>, term: Long): Boolean {
        if (logEntry.term != term) {
            return false
        }

        val followers = members.toMutableSet()
        followers.remove(Server.getPID())
        val followersNeeded = (followers.size + 1) / 2
        val followersUpToDate = followers.count {
            val v = this.followerMap!![it] ?: return false
            logEntry.index <= v.second
        }
        return followersNeeded <= followersUpToDate
    }

    fun decrementFollowerNextIndex(follower: PID) {
        val (nextIndex, replIndex) = this.followerMap!![follower] ?: return
        this.followerMap!![follower] = Pair(nextIndex - 1, replIndex)
    }

    fun addEntry(entryFn: (index: Long) -> LogEntry): LogEntry {
        this.maxIndex += 1
        val entry = entryFn(this.maxIndex)
        this.logMap[this.maxIndex] = entry
        this.cleanOldLogs()
        return entry
    }

    suspend fun addLeaderElectedEntry(members: Members, term: Long): LogEntry {
        val followerIndexes = Pair(this.maxIndex + 1, 0L)
        val followers = members.otherMembers()
        this.followerMap = this.followerMap ?: HashMap()
        this.followerMap!!.clear()
        this.followerMap!!.putAll(followers.map { Pair(it, followerIndexes) }.toMap())
        val thisPID = Server.getPID()
        return this.addEntry { index ->
            LogEntry(index, term,
                    LogEntry.LogValue.LeaderElected(thisPID, followers.toTypedArray()))
        }
    }

    fun addAddFollowerEntry(term: Long, follower: PID): LogEntry {
        this.followerMap!![follower] = Pair(this.maxIndex + 1, 0)
        return this.addEntry { index ->
            LogEntry(index, term, LogEntry.LogValue.AddFollower(follower))
        }
    }

    fun addRemoveFollowerEntry(term: Long, follower: PID): LogEntry {
        return this.addEntry { index ->
            LogEntry(index, term, LogEntry.LogValue.RemoveFollower(follower))
        }
    }

    fun hasGivenPrevLog(logInfo: LogInfo): Boolean {
        val entry = this.logMap[logInfo.index] ?: return logInfo.index == 0L
        return entry.term == logInfo.term
    }

    fun appendEntries(members: Members,
                      entries: List<LogEntry>,
                      leaderCommittedIndex: Long): List<LogEntry> {
        val notIncludedEntries = arrayListOf<LogEntry>()

        for (entry in entries) {
            val e = this.logMap[entry.index]
            if (e == entry) {
                continue
            }

            notIncludedEntries.add(entry)

            this.logMap[entry.index] = entry

            if (e != null) {
                members.maybeResetMembershipChange(entry)
            }
        }

        for (entry in notIncludedEntries) {
            members.applyMissingEntry(entry)
        }

        val oCommittedIndex = this.committedIndex
        this.maxIndex = max(entries.lastOrNull()?.index ?: this.maxIndex, this.maxIndex)
        this.committedIndex = max(this.committedIndex, leaderCommittedIndex)
        this.cleanOldLogs()
        return entriesBetween(this.logMap, oCommittedIndex + 1, this.committedIndex)
    }

    fun isCandidateLogUpToDate(candidateLogInfo: LogInfo): Boolean {
        val maxLogEntry = this.logMap[this.maxIndex]!!
        if (maxLogEntry.term == candidateLogInfo.term) {
            return maxLogEntry.index <= candidateLogInfo.index
        }
        return maxLogEntry.term < candidateLogInfo.term
    }

    fun lastEntry(): LogEntry? {
        return this.logMap[this.maxIndex]
    }

    fun lastCommitted(): LogEntry? {
        return this.logMap[this.committedIndex]
    }

    private fun cleanOldLogs() {
        val newMinIndex = this.committedIndex - 99
        if (newMinIndex > this.minIndex) {
            for (idx in this.minIndex..newMinIndex + 1) {
                this.logMap.remove(idx)
            }
            this.minIndex = newMinIndex
        }
    }

    sealed class LogEntryError(message: String?) : Throwable(message) {
        object FollowerDoesntExist : LogEntryError("Follower doesn't exist")
        object FollowerBehind : LogEntryError("The follower is too far behind")
    }

    companion object {
        fun forLonelyLeader(lastEntry: LogEntry, toAppend: List<LogEntry>): Logs {
            val logs = Logs(lastEntry)
            logs.followerMap = HashMap()
            logs.logMap.putAll(toAppend.map { Pair(it.index, it) })
            return logs
        }

        fun forNewFollower(lastEntry: LogEntry): Logs = Logs(lastEntry)
    }
}