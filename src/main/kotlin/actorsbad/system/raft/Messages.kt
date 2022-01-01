package actorsbad.system.raft

import kotlinx.datetime.Instant
import actorsbad.system.MonitorRef
import actorsbad.system.PID
import actorsbad.system.utils.SInstant
import java.io.Serializable

sealed class AsyncMessage : Serializable {
    class AppendEntriesRequest(
            val leaderPID: PID,
            val term: Long,
            val previousLog: LogInfo,
            val entries: Array<LogEntry>,
            // last index committed by the leader
            val leaderCommittedIndex: Long,
            leaderTimestamp: Instant,
    ) : AsyncMessage() {
        private val leaderTimestampS = SInstant(leaderTimestamp.toEpochMilliseconds())
        val leaderTimestamp
            get() = this.leaderTimestampS.inner
    }

    class AppendEntriesResponse(
            val from: PID,
            val term: Long,
            val success: Boolean,
            val replicatedLogIndex: Long?,
            leaderTimestamp: Instant,
    ) : AsyncMessage() {
        private val leaderTimestampS = SInstant(leaderTimestamp.toEpochMilliseconds())
        val leaderTimestamp
            get() = this.leaderTimestampS.inner
    }

    data class RequestVoteRequest(
            val term: Long,
            val candidatePID: PID,
            val lastLog: LogInfo,
            val replacingLeader: Boolean,
    ) : AsyncMessage()

    data class RequestVoteResponse(
            val from: PID,
            val term: Long,
            val voteGranted: Boolean,
    ) : AsyncMessage()

    class InstallSnapshot(
            val members: Members,
            val term: Long,
            val lastCommittedEntry: LogEntry,
            val config: Config,
            val data: Serializable,
            val commandTracker: CommandTracker,
    ) : AsyncMessage()

    data class TimeoutNow(
            val appendEntriesRequest: AppendEntriesRequest,
    ) : AsyncMessage()

    object RemoveFollowerCompleted : AsyncMessage()
}

data class SyncMessage(val fromPID: PID, val ref: MonitorRef, val value: Value) : Serializable {
    sealed class Value: Serializable {
        data class Command(val arg: Serializable, val id: Long) : Value()
        data class Query(val arg: Serializable) : Value()
        data class QueryAny(val arg: Serializable) : Value()
        data class ChangeConfig(val config: Config) : Value()
        data class AddFollower(val pid: PID) : Value()
        data class RemoveFollower(val pid: PID) : Value()
        data class ReplaceLeader(val pid: PID) : Value()
    }
}