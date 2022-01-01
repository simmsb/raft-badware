package actorsbad.system.raft

import kotlinx.coroutines.withTimeoutOrNull
import actorsbad.system.*
import actorsbad.system.messages.ProcessExitMessage
import java.io.Serializable
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.coroutineContext
import kotlin.system.exitProcess

// TODO: split these into states

sealed class AddFollowerException(message: String) : Throwable(message) {
    object NoLeaderFound : AddFollowerException("No leader was found that we could join")
}

sealed class ServerResponse : Serializable {
    data class MessageResponse(val msg: AsyncMessage) : ServerResponse()
    data class CommandOrQueryResponse(val response: Serializable) : ServerResponse()
    object Ok : ServerResponse()
    sealed class Error : ServerResponse() {
        object UnexpectedMessage : Error()
        data class NotLeader(val leader: PID?) : Error()
        object NotRunning : Error()
        data class Errored(val error: Any) : Error()
        data class Exited(val pid: PID, val reason: Any?) : Error()
        data class BadResponse(val response: Any) : Error()
    }
}

internal suspend fun PID.callServer(msg: SyncMessage.Value): ServerResponse {
    return withTimeoutOrNull(5000) {
        this@callServer.callServerInner(msg)
    } ?: ServerResponse.Error.NotRunning
}

internal suspend fun PID.callServerInner(msg: SyncMessage.Value): ServerResponse {
    val monitorRef = Registry.linkSingleDirection(this, Process.self())
            ?: return ServerResponse.Error.NotRunning

    val sMsg = SyncMessage(Process.self(), monitorRef, msg)

    if (!this.send(sMsg))
        return ServerResponse.Error.NotRunning
    try {
        return when (val r = Process.receive()) {
            is Pair<*, *> -> {
                Registry.unMonitor(r.second as MonitorRef)
                r.first as ServerResponse
            }

            is ProcessExitMessage ->
                ServerResponse.Error.Exited(r.exitedProcess, r.reason)

            else ->
                ServerResponse.Error.BadResponse(r)
        }
    } finally {
        Registry.unMonitor(monitorRef)
    }
}

internal class ServerData<C : Serializable, Q : Serializable, R : Serializable, D : Data<C, Q, R>>(
        var members: Members,
        var currentTerm: Long,
        var leadership: Leader?,
        var election: Election,
        var logs: Logs,
        var data: D,
        var commandTracker: CommandTracker,
        var config: Config,
)

private sealed class OnStart<D> {
    class NewGroup<D>(val config: Config, val ctor: () -> D) : OnStart<D>()
    class JoinGroup<D>(val knownMembers: List<PID>) : OnStart<D>()
}

class Server<C : Serializable, Q : Serializable, R : Serializable, D : Data<C, Q, R>> private constructor(
        private var onStart: OnStart<D>?,
        initialState: State,
) : StateMachine<State>(initialState) {
    private lateinit var sData: ServerData<C, Q, R, D>

    // nauseating
    var members: Members
        get() = this.sData.members
        set(value) {
            println("${this.self()} changing members")
            this.sData.members = value
        }
    var currentTerm: Long
        get() = this.sData.currentTerm
        set(value) {
            this.sData.currentTerm = value
        }
    var leadership: Leader?
        get() = this.sData.leadership
        set(value) {
            this.sData.leadership = value
        }
    var election: Election
        get() = this.sData.election
        set(value) {
            this.sData.election = value
        }
    var logs: Logs
        get() = this.sData.logs
        set(value) {
            this.sData.logs = value
        }
    var data: D
        get() = this.sData.data
        set(value) {
            this.sData.data = value
        }
    var commandTracker: CommandTracker
        get() = this.sData.commandTracker
        set(value) {
            this.sData.commandTracker = value
        }
    var config: Config
        get() = this.sData.config
        set(value) {
            this.sData.config = value
        }

    override suspend fun mainFn() {
        val onStart = this.onStart
        this.onStart = null
        try {
            when (onStart) {
                is OnStart.NewGroup ->
                    this.sData = newGroupInit(onStart.config, onStart.ctor)

                is OnStart.JoinGroup ->
                    this.sData = joinGroupInit(onStart.knownMembers)

                null -> throw Exception("not possible")
            }
        } catch (e: AddFollowerException) {
            // bit of a hack, just die and try again
            exitProcess(0)
        }

        super.mainFn()
    }

    companion object {
        private suspend fun <C : Serializable, Q : Serializable, R : Serializable, D : Data<C, Q, R>> newGroupInit(config: Config, ctor: () -> D): ServerData<C, Q, R, D> {
            val snapshot = Snapshot.forLonelyLeader(config, ctor)
            val logs = Logs.forLonelyLeader(snapshot.lastCommittedEntry, listOf())
            return this.fromSnapshot(snapshot, logs)
        }

        fun <C : Serializable, Q : Serializable, R : Serializable, D : Data<C, Q, R>> newGroup(config: Config, ctor: () -> D): Server<C, Q, R, D> {
            return Server(OnStart.NewGroup(config, ctor), State.Leader)
        }

        private suspend fun <C : Serializable, Q : Serializable, R : Serializable, D : Data<C, Q, R>> joinGroupInit(knownMembers: List<PID>): ServerData<C, Q, R, D> {
            val snapshot = this.doAddServer(knownMembers)
            println("joined with snapshot: $snapshot")
            val logs = Logs.forNewFollower(snapshot.lastCommittedEntry)
            val election = Election.forFollower(snapshot.config)

            @Suppress("UNCHECKED_CAST")
            return ServerData(
                    snapshot.members,
                    snapshot.term,
                    null,
                    election,
                    logs,
                    snapshot.data as D,
                    snapshot.commandTracker,
                    snapshot.config,
            )
        }

        fun <C : Serializable, Q : Serializable, R : Serializable, D : Data<C, Q, R>> joinGroup(knownMembers: List<PID>): Server<C, Q, R, D> {
            return Server(OnStart.JoinGroup(knownMembers), State.Follower)
        }

        @Throws(AddFollowerException::class)
        private suspend fun doAddServer(knownMembers: List<PID>): Snapshot {
            val q = ArrayDeque(knownMembers)
            while (true) {
                val maybeLeader = q.removeFirstOrNull() ?: break
                when (val r = maybeLeader.callServer(SyncMessage.Value.AddFollower(this.getPID()))) {
                    is ServerResponse.MessageResponse -> when (r.msg) {
                        is AsyncMessage.InstallSnapshot ->
                            return Snapshot.fromInstallSnapShot(r.msg)
                        else -> continue
                    }

                    is ServerResponse.Error.NotLeader ->
                        if (r.leader != null) {
                            q.remove(r.leader)
                            q.addFirst(r.leader)
                        }

                    else -> continue
                }
            }

            throw AddFollowerException.NoLeaderFound
        }

        private suspend fun <C : Serializable, Q : Serializable, R : Serializable, D : Data<C, Q, R>> fromSnapshot(snapshot: Snapshot, logs: Logs): ServerData<C, Q, R, D> {
            @Suppress("UNCHECKED_CAST")
            return ServerData(
                    snapshot.members,
                    snapshot.term,
                    Leader.new(snapshot.config),
                    Election.forLeader(),
                    logs,
                    snapshot.data as D,
                    snapshot.commandTracker,
                    snapshot.config,
            )
        }

        suspend fun getPID(): PID {
            return coroutineContext[RaftPIDContext]!!.pid
        }
    }

    override fun extraCoroContexts(): CoroutineContext = RaftPIDContext(this.self())

    override suspend fun handleEvent(state: State, message: Any): StateMachineResponse<State> {
//        println("${getPID()}: handling event: $message in state: $state")
        val r = state.handle(message, this)
//        println("${getPID()}: done handling event in state: $state")
        return r
    }
}