package actorsbad.system.raft

import kotlinx.datetime.Clock
import kotlinx.datetime.Instant
import actorsbad.system.MonitorRef
import actorsbad.system.PID
import actorsbad.system.StateMachineResponse
import java.io.Serializable

private suspend fun reply(fromPID: PID, ref: MonitorRef, response: ServerResponse) {
//    println("replying to $fromPID with $response")
    fromPID.send(Pair(response, ref))
}

abstract class EventHandler<State> {
    suspend fun handle(message: Any, server: Server<*, *, *, *>): StateMachineResponse<State> {
        return when (message) {
            is AsyncMessage -> this.handleAsyncMessage(message, server)
            is SyncMessage -> this.handleSyncMessage(message, server)
            is Timeouts -> this.handleTimeout(message, server)
            else -> {
                println("unknown event: $message")
                StateMachineResponse.SameState
            }
        }
    }

    protected abstract suspend fun handleAsyncMessage(message: AsyncMessage, server: Server<*, *, *, *>): StateMachineResponse<State>
    protected abstract suspend fun handleSyncMessage(message: SyncMessage, server: Server<*, *, *, *>): StateMachineResponse<State>
    protected abstract suspend fun handleTimeout(timeout: Timeouts, server: Server<*, *, *, *>): StateMachineResponse<State>
}

suspend fun maybeBecomeFollower(message: AsyncMessage,
                                term: Long,
                                server: Server<*, *, *, *>): StateMachineResponse<State>? {
    if (term > server.currentTerm) {
        server.becomeFollower(term)

//        println("becoming follower and reprocessing: $message")
        return StateMachineResponse.NextStateWithEvents(State.Follower, listOf(message))
    }

    return null
}


suspend fun Server<*, *, *, *>.becomeFollower(term: Long) {
    println("${Server.getPID()} is becoming a follower")
    this.leadership?.stop()
    this.members.putLeader(null)
    this.election.updateForFollower(this.config)
    this.leadership = null
    this.currentTerm = term
}

suspend fun Server<*, *, *, *>.becomeLeader() {
    println("${Server.getPID()} is becoming a leader")
    this.leadership?.stop()
    this.leadership = Leader.new(this.config)
    val entry = this.logs.addLeaderElectedEntry(this.members, this.currentTerm)
    // TODO: persist
    this.members.putLeader(Server.getPID())
    this.broadcastAppendEntries()
}

suspend fun Server<*, *, *, *>.broadcastAppendEntries() {
    val followers = this.members.otherMembers()
//    println("${Server.getPID()} broadcasting append entries to: $followers")
    if (followers.isEmpty()) {
        val toApply = this.logs.commit()
        this.leadership!!.resetQuorumTimer(this.config)
        toApply.forEach { this.applyCommittedLogAsLeader(it) }
    } else {
        val now = Clock.System.now()
        followers.forEach { this.sendAppendEntries(it, now) }
    }
    this.leadership!!.resetHeartbeatTimer(this.config)
}

suspend fun Server<*, *, *, *>.applyCommittedLogAsLeader(entry: LogEntry) {
    when (entry.value) {
        is LogEntry.LogValue.Command ->
            this.runCommand(entry.value.from, entry.value.ref, entry.value.argument, entry.value.id, true)

        is LogEntry.LogValue.Query ->
            this.runQuery(entry.value.from, entry.value.ref, entry.value.argument)

        is LogEntry.LogValue.ChangeConfig ->
            this.config = entry.value.config

        is LogEntry.LogValue.LeaderElected -> return
            // TODO: hooks?

        is LogEntry.LogValue.AddFollower ->
            // TODO: hooks?
            this.members.commitMembershipChange(entry.index)

        is LogEntry.LogValue.RemoveFollower -> {
            entry.value.follower.send(AsyncMessage.RemoveFollowerCompleted)
            this.members.commitMembershipChange(entry.index)
        }

        is LogEntry.LogValue.RestoreFromFiles ->
            return
    }
}

suspend fun Server<*, *, *, *>.applyCommittedLogAsNonLeader(entry: LogEntry) {
//    println("applying logs as non leader: $entry")
    when (entry.value) {
        is LogEntry.LogValue.Command ->
            this.runCommand(entry.value.from, entry.value.ref, entry.value.argument, entry.value.id, false)

        is LogEntry.LogValue.ChangeConfig ->
            this.config = entry.value.config

        is LogEntry.LogValue.AddFollower ->
            this.members.commitMembershipChange(entry.index)

        is LogEntry.LogValue.RemoveFollower ->
            this.members.commitMembershipChange(entry.index)

        else -> return
    }
}

suspend fun <C: Serializable, Q: Serializable, R: Serializable, D: Data<C, Q, R>>
        Server<C, Q, R, D>.runCommand(fromPID: PID, ref: MonitorRef, arg: Any, id: Long, asLeader: Boolean) {
    this.commandTracker.get(id) ?.let {
        if (asLeader)
            reply(fromPID, ref, ServerResponse.CommandOrQueryResponse(it))
        return
    }

    @Suppress("UNCHECKED_CAST")
    val r = this.data.command(arg as C)

    this.commandTracker.put(id, r, this.config.maxRetainedCommandResults)

    if (asLeader) {
        reply(fromPID, ref, ServerResponse.CommandOrQueryResponse(r))
        // TODO: hooks?
    }
}

suspend fun <C: Serializable, Q: Serializable, R: Serializable, D: Data<C, Q, R>>
        Server<C, Q, R, D>.runQuery(fromPID: PID, ref: MonitorRef, arg: Any) {
    @Suppress("UNCHECKED_CAST")
    val r = this.data.query(arg as Q)

    reply(fromPID, ref, ServerResponse.CommandOrQueryResponse(r))
    // TODO: hooks?
}

suspend fun <C: Serializable, Q: Serializable, R: Serializable, D: Data<C, Q, R>>
        Server<C, Q, R, D>.runQueryAny(fromPID: PID, ref: MonitorRef, arg: Any) {
    @Suppress("UNCHECKED_CAST")
    val r = this.data.query(arg as Q)

    reply(fromPID, ref, ServerResponse.CommandOrQueryResponse(r))
    // TODO: hooks?
}

suspend fun Server<*, *, *, *>.sendAppendEntries(follower: PID, now: Instant) {
    try {
        val aer = this.logs.constructAppendEntries(
                this.currentTerm, follower, now
        )

        follower.send(aer)
    } catch (e: Logs.LogEntryError.FollowerBehind) {
        this.sendSnapshot(follower) { follower.send(it) }
    } catch (e: Exception) {
        e.printStackTrace()
        // do nothing
    }
}

suspend fun Server<*, *, *, *>.broadcastVoteRequest(replaceLeader: Boolean) {
    println("broadcasting vote req")
    val others = this.members.otherMembers()
    others.forEach {
        val lastEntryInfo = this.logs.lastEntry()!!.info()
        it.send(AsyncMessage.RequestVoteRequest(this.currentTerm,
                Server.getPID(), lastEntryInfo, replaceLeader
        ))
    }
}

suspend fun <D: Data<*, *, *>> Server<*, *, *, D>.installSnapshot(snapshot: Snapshot) {
    println("installing snapshot")
    this.logs = Logs.forNewFollower(snapshot.lastCommittedEntry)
    this.members = snapshot.members
    this.currentTerm = snapshot.term
    @Suppress("UNCHECKED_CAST")
    this.data = snapshot.data as D
    this.commandTracker = snapshot.commandTracker
    this.election.resetTimer(this.config)
}

fun <D: Data<*, *, *>> Server<*, *, *, D>.makeInstallSnapshot(): AsyncMessage.InstallSnapshot {
    val lastEntry = this.logs.lastCommitted()!!
    return AsyncMessage.InstallSnapshot(
            this.members,
            this.currentTerm,
            lastEntry,
            this.config,
            this.data,
            this.commandTracker
    )
}

suspend fun Server<*, *, *, *>.sendSnapshot(follower: PID, function: suspend (AsyncMessage) -> Unit) {
    // TODO: persistence
    function(this.makeInstallSnapshot())
}

suspend fun handleRequestVoteRequest(message: AsyncMessage.RequestVoteRequest, server: Server<*, *, *, *>, state: State): StateMachineResponse<State> {
    if (message.replacingLeader || leaderAuthorityTimedOut(state, server)) {
        maybeBecomeFollower(message, message.term, server) ?.let { return it }

        val shouldWeGrantVote = (message.term == server.currentTerm
                && server.election.votedFor in listOf(null, message.candidatePID)
                && server.logs.isCandidateLogUpToDate(message.lastLog))
        val resp = AsyncMessage.RequestVoteResponse(Server.getPID(), server.currentTerm, shouldWeGrantVote)
        message.candidatePID.send(resp)
        if (shouldWeGrantVote)
            server.election.voteFor(message.candidatePID, server.config)
    } else {
        val resp = AsyncMessage.RequestVoteResponse(Server.getPID(), server.currentTerm, false)
        message.candidatePID.send(resp)
    }

    return StateMachineResponse.NextState(state)
}

fun leaderAuthorityTimedOut(state: State, server: Server<*, *, *, *>): Boolean {
    return when (state) {
        is State.Leader ->
            server.leadership!!.hasLeaseExpired(server.members, server.config)
        else ->
            server.election.shouldInitiateElection(server.config)
    }
}

suspend fun handleAppendEntries(ap: AsyncMessage.AppendEntriesRequest, server: Server<*, *, *, *>): StateMachineResponse<State> {
    if (ap.term < server.currentTerm) {
        ap.leaderPID.send(AsyncMessage.AppendEntriesResponse(Server.getPID(), server.currentTerm, false,
                null, ap.leaderTimestamp))
        return StateMachineResponse.SameState
    }

    println("applying logs: ${ap.entries.toList()}, prevlog: ${ap.previousLog}")

    if (server.logs.hasGivenPrevLog(ap.previousLog)) {
        val toApply = server.logs.appendEntries(server.members, ap.entries.toList(), ap.leaderCommittedIndex)
//        println("has log, we can apply: $toApply")
        server.members.putLeader(ap.leaderPID)
        server.currentTerm = ap.term

        toApply.forEach {
            server.applyCommittedLogAsNonLeader(it)
        }
        // TODO: persist
        ap.leaderPID.send(AsyncMessage.AppendEntriesResponse(Server.getPID(), ap.term, true,
                server.logs.maxIndex, ap.leaderTimestamp))
    } else {
        ap.leaderPID.send(AsyncMessage.AppendEntriesResponse(Server.getPID(), server.currentTerm, false,
                null, ap.leaderTimestamp))
        server.members.putLeader(ap.leaderPID)
        server.currentTerm = ap.term
    }

    server.election.resetTimer(server.config)
    return StateMachineResponse.NextState(State.Follower)
}

suspend fun becomeCandidateAndInitiateElection(server: Server<*, *, *, *>, replaceLeader: Boolean): StateMachineResponse<State> {
    val allMembers = server.members.allMembers()
    return if (allMembers.size == 1 && Server.getPID() in allMembers) {
        println("${server.self()} is becoming a candidate but now a leader")
        server.currentTerm += 1
        server.election = Election.forLeader()
        server.becomeLeader()
        StateMachineResponse.NextState(State.Leader)
    } else {
        server.members.removeDeadMembers()
        println("${server.self()} is becoming a candidate")
        server.members.putLeader(null)
        server.election.updateForCandidate(server.config)
        server.currentTerm += 1
        server.broadcastVoteRequest(replaceLeader)
        StateMachineResponse.NextState(State.Candidate)
    }
}

sealed class State : EventHandler<State>() {
    object Leader : State() {
        override suspend fun handleAsyncMessage(message: AsyncMessage, server: Server<*, *, *, *>): StateMachineResponse<State> {
            return when (message) {
                is AsyncMessage.AppendEntriesResponse ->
                    this.handleAppendEntriesResponse(message, server)

                is AsyncMessage.RequestVoteRequest ->
                    handleRequestVoteRequest(message, server, Leader)

                is AsyncMessage.AppendEntriesRequest ->
                    maybeBecomeFollower(message, message.term, server) ?: StateMachineResponse.SameState

                is AsyncMessage.RequestVoteResponse ->
                    maybeBecomeFollower(message, message.term, server) ?: StateMachineResponse.SameState

                else ->
                    StateMachineResponse.SameState
            }
        }

        private suspend fun handleAppendEntriesResponse(message: AsyncMessage.AppendEntriesResponse, server: Server<*, *, *, *>): StateMachineResponse<State> {
            maybeBecomeFollower(message, message.term, server)?.let { return it }

            server.leadership!!.onFollwerResponded(server.members, message.from,
                    message.leaderTimestamp, server.config)

            if (message.success) {
                val toApply = server.logs.setFollowerIndex(
                        server.members, server.currentTerm, message.from, message.replicatedLogIndex!!)

                for (entry in toApply) {
                    server.applyCommittedLogAsLeader(entry)
                }

                if (server.members.pendingLeaderChange == message.from) {
                    return try {
                        val aer = server.logs.constructAppendEntries(
                                server.currentTerm, message.from, Clock.System.now()
                        )

                        message.from.send(AsyncMessage.TimeoutNow(aer))
                        server.becomeFollower(server.currentTerm)
                        StateMachineResponse.NextState(Follower)
                    } catch (e: Logs.LogEntryError.FollowerBehind) {
                        StateMachineResponse.SameState
                    }
                }

                return StateMachineResponse.SameState
            } else {
                server.logs.decrementFollowerNextIndex(message.from)
                server.sendAppendEntries(message.from, Clock.System.now())
                return StateMachineResponse.SameState
            }
        }

        override suspend fun handleSyncMessage(message: SyncMessage, server: Server<*, *, *, *>): StateMachineResponse<State> {
            return when (message.value) {
                is SyncMessage.Value.Command ->
                    this.handleCommand(message.value, message.fromPID, message.ref, server)

                is SyncMessage.Value.Query ->
                    this.handleQuery(message.value, message.fromPID, message.ref, server)

                is SyncMessage.Value.ChangeConfig ->
                    this.handleChangeConfig(message.value, message.fromPID, message.ref, server)

                is SyncMessage.Value.AddFollower ->
                    this.handleAddFollower(message.value, message.fromPID, message.ref, server)

                is SyncMessage.Value.RemoveFollower ->
                    this.handleRemoveFollower(message.value, message.fromPID, message.ref, server)

                is SyncMessage.Value.ReplaceLeader ->
                    this.handleReplaceLeader(message.value, message.fromPID, message.ref, server)

                else ->
                    super.handleSyncMessage(message, server)
            }
        }

        private suspend fun handleCommand(message: SyncMessage.Value.Command, fromPID: PID,
                                          ref: MonitorRef, server: Server<*, *, *, *>):
                StateMachineResponse<State> {
            val entry = server.logs.addEntry {
                LogEntry(it, server.currentTerm, LogEntry.LogValue.Command(fromPID, ref, message.arg, message.id))
            }

            // TODO: Persist entries
            server.broadcastAppendEntries()

            return StateMachineResponse.SameState
        }

        private suspend fun handleQuery(value: SyncMessage.Value.Query, fromPID: PID, ref: MonitorRef, server: Server<*, *, *, *>): StateMachineResponse<State> {
            if (server.leadership!!.hasLeaseExpired(server.members, server.config)) {
                val entry = server.logs.addEntry {
                    LogEntry(it, server.currentTerm, LogEntry.LogValue.Query(fromPID, ref, value.arg))
                }

                server.broadcastAppendEntries()
            } else {
                server.runQuery(fromPID, ref, value.arg)
            }

            return StateMachineResponse.SameState
        }

        private suspend fun handleChangeConfig(value: SyncMessage.Value.ChangeConfig, fromPID: PID, ref: MonitorRef, server: Server<*, *, *, *>): StateMachineResponse<State> {
            val entry = server.logs.addEntry {
                LogEntry(it, server.currentTerm, LogEntry.LogValue.ChangeConfig(value.config))
            }

            reply(fromPID, ref, ServerResponse.Ok)

            return StateMachineResponse.SameState
        }

        private suspend fun handleAddFollower(value: SyncMessage.Value.AddFollower, fromPID: PID, ref: MonitorRef, server: Server<*, *, *, *>): StateMachineResponse<State> {
            val entry = try {
                server.members.addFollower(value.pid) {
                    server.logs.addAddFollowerEntry(server.currentTerm, value.pid)
                }
            } catch (e: Members.MemberManipulationError) {
                reply(fromPID, ref, ServerResponse.Error.Errored(e))
                return StateMachineResponse.SameState
            }

            // TODO: persist log
            // TODO: snapshots
            server.sendSnapshot(value.pid) {
                reply(fromPID, ref, ServerResponse.MessageResponse(it))
            }
            server.broadcastAppendEntries()
            return StateMachineResponse.SameState
        }

        private suspend fun handleRemoveFollower(value: SyncMessage.Value.RemoveFollower, fromPID: PID, ref: MonitorRef, server: Server<*, *, *, *>): StateMachineResponse<State> {
            val entry = try {
                server.members.removeFollower(value.pid) {
                    if (!server.leadership!!.canRemoveFollower(server.members, value.pid, server.config))
                        throw Members.MemberManipulationError.WouldBreakQuorum

                    server.logs.addRemoveFollowerEntry(server.currentTerm, value.pid)
                }
            } catch (e: Members.MemberManipulationError) {
                reply(fromPID, ref, ServerResponse.Error.Errored(e))
                return StateMachineResponse.SameState
            }

            server.leadership!!.removeFollowerResponseTime(value.pid)

            // TODO: persist log
            // TODO: snapshots
            server.broadcastAppendEntries()
            reply(fromPID, ref, ServerResponse.Ok)
            return StateMachineResponse.SameState
        }

        private suspend fun handleReplaceLeader(value: SyncMessage.Value.ReplaceLeader, fromPID: PID, ref: MonitorRef, server: Server<*, *, *, *>): StateMachineResponse<State> {
            try {
                server.members.replaceLeader(value.pid) {
                    if (value.pid in server.leadership!!.unresponsiveFollowers(server.members, server.config))
                        throw Members.MemberManipulationError.LeaderUnresponsive
                }
            } catch (e: Members.MemberManipulationError) {
                reply(fromPID, ref, ServerResponse.Error.Errored(e))
                return StateMachineResponse.SameState
            }

            reply(fromPID, ref, ServerResponse.Ok)
            return StateMachineResponse.SameState
        }

        override suspend fun handleTimeout(timeout: Timeouts, server: Server<*, *, *, *>): StateMachineResponse<State> {
            return when (timeout) {
                is Timeouts.HeartbeatTimeout -> {
                    server.broadcastAppendEntries()
                    StateMachineResponse.SameState
                }
                is Timeouts.QuorumTimeout -> {
                    server.becomeFollower(server.currentTerm)
                    StateMachineResponse.NextState(Follower)
                }
                else ->
                    StateMachineResponse.SameState
            }
        }
    }

    object Candidate : State() {
        override suspend fun handleAsyncMessage(message: AsyncMessage, server: Server<*, *, *, *>): StateMachineResponse<State> {
            return when (message) {
                is AsyncMessage.AppendEntriesRequest ->
                    handleAppendEntries(message, server)

                is AsyncMessage.AppendEntriesResponse ->
                    maybeBecomeFollower(message, message.term, server) ?: StateMachineResponse.SameState

                is AsyncMessage.RequestVoteRequest ->
                    handleRequestVoteRequest(message, server, Candidate)

                is AsyncMessage.RequestVoteResponse ->
                    this.handleRequestVoteResponse(message, server)

                else -> StateMachineResponse.SameState
            }
        }

        private suspend fun handleRequestVoteResponse(message: AsyncMessage.RequestVoteResponse, server: Server<*, *, *, *>): StateMachineResponse<State> {
            maybeBecomeFollower(message, message.term, server) ?.let { return it }

            if (message.term < server.currentTerm || !message.voteGranted)
                return StateMachineResponse.SameState

            val weHaveMajority = server.election.gainVote(server.members, message.from)

            if (weHaveMajority) {
                server.becomeLeader()
                return StateMachineResponse.NextState(Leader)
            }

            return StateMachineResponse.SameState
        }

        override suspend fun handleTimeout(timeout: Timeouts, server: Server<*, *, *, *>): StateMachineResponse<State> {
            return when (timeout) {
                is Timeouts.ElectionTimeout ->
                    becomeCandidateAndInitiateElection(server, false)

                else ->
                    StateMachineResponse.SameState
            }
        }
    }

    object Follower : State() {
        override suspend fun handleAsyncMessage(message: AsyncMessage, server: Server<*, *, *, *>): StateMachineResponse<State> {
            return when (message) {
                is AsyncMessage.AppendEntriesRequest ->
                    handleAppendEntries(message, server)

                is AsyncMessage.AppendEntriesResponse ->
                    maybeBecomeFollower(message, message.term, server) ?: StateMachineResponse.SameState

                is AsyncMessage.RequestVoteRequest ->
                    handleRequestVoteRequest(message, server, Follower)

                is AsyncMessage.RequestVoteResponse ->
                    maybeBecomeFollower(message, message.term, server) ?: StateMachineResponse.SameState

                is AsyncMessage.TimeoutNow ->
                    this.handleTimeoutNow(message, server)

                is AsyncMessage.InstallSnapshot -> {
                    server.installSnapshot(Snapshot.fromInstallSnapShot(message))
                    StateMachineResponse.SameState
                }

                is AsyncMessage.RemoveFollowerCompleted ->
                    StateMachineResponse.Stop
            }
        }

        private suspend fun handleTimeoutNow(message: AsyncMessage.TimeoutNow, server: Server<*, *, *, *>): StateMachineResponse<State> {
            if (message.appendEntriesRequest.term == server.currentTerm &&
                    server.logs.hasGivenPrevLog(message.appendEntriesRequest.previousLog)) {
                val toApply = server.logs.appendEntries(server.members,
                        message.appendEntriesRequest.entries.asList(),
                        message.appendEntriesRequest.leaderCommittedIndex
                )

                toApply.forEach { server.applyCommittedLogAsNonLeader(it) }
                // TODO: persist
                return becomeCandidateAndInitiateElection(server, true)
            }

            return StateMachineResponse.SameState
        }

        override suspend fun handleTimeout(timeout: Timeouts, server: Server<*, *, *, *>): StateMachineResponse<State> {
            return when (timeout) {
                is Timeouts.ElectionTimeout ->
                    becomeCandidateAndInitiateElection(server, false)

                else ->
                    StateMachineResponse.SameState
            }
        }
    }

    override suspend fun handleSyncMessage(message: SyncMessage, server: Server<*, *, *, *>): StateMachineResponse<State> {
        return when (message.value) {
            is SyncMessage.Value.QueryAny -> {
                server.runQueryAny(message.fromPID, message.ref, message.value.arg)
                StateMachineResponse.SameState
            }
            else -> {
                val error = if (this is Leader) {
                    ServerResponse.Error.UnexpectedMessage
                } else {
                    ServerResponse.Error.NotLeader(server.members.leader)
                }
                reply(message.fromPID, message.ref, error)
                StateMachineResponse.SameState
            }
        }
    }
}