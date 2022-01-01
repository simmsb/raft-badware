package actorsbad.system.raft

import kotlinx.coroutines.withContext
import actorsbad.system.PID
import actorsbad.system.Registry
import java.io.Serializable
import kotlin.time.ExperimentalTime
import kotlin.time.milliseconds

data class RaftError(val reason: ServerResponse.Error): Throwable()

class Raft<C : Serializable, Q : Serializable, R : Serializable, D : Data<C, Q, R>>
private constructor(val server: Server<C, Q, R, D>, name: String) {
    val name: String = name

    fun isLeader(): Boolean = this.server.leadership != null

    suspend fun invokeCommand(who: PID, cmd: C): R {
        return withContext(this.server.extraCoroContexts()) {
            @Suppress("UNCHECKED_CAST")
            when (val r = who.callServer(SyncMessage.Value.Command(cmd, Registry.generateUniqueRef()))) {
                is ServerResponse.CommandOrQueryResponse ->
                    return@withContext r.response as R
                is ServerResponse.Error ->
                    throw RaftError(r)
                else ->
                    throw RaftError(ServerResponse.Error.BadResponse(r))
            }
        }
    }

    suspend fun invokeCommand(cmd: C): R {
        return withContext(this.server.extraCoroContexts()) {
            val leader = this@Raft.server.members.leader ?: throw RaftError(ServerResponse.Error.NotRunning)

            this@Raft.invokeCommand(leader, cmd)
        }
    }

    suspend fun invokeQuery(who: PID, cmd: Q): R {
        return withContext(this.server.extraCoroContexts()) {
            @Suppress("UNCHECKED_CAST")
            when (val r = who.callServer(SyncMessage.Value.Query(cmd))) {
                is ServerResponse.CommandOrQueryResponse ->
                    return@withContext r.response as R
                is ServerResponse.Error ->
                    throw RaftError(r)
                else ->
                    throw RaftError(ServerResponse.Error.BadResponse(r))
            }
        }
    }

    suspend fun invokeQuery(cmd: Q): R {
        return withContext(this.server.extraCoroContexts()) {
            val leader = this@Raft.server.members.leader ?: throw RaftError(ServerResponse.Error.NotRunning)

            this@Raft.invokeQuery(leader, cmd)
        }
    }

    suspend fun invokeQueryAny(who: PID, cmd: Q): R {
        return withContext(this.server.extraCoroContexts()) {
            @Suppress("UNCHECKED_CAST")
            when (val r = who.callServer(SyncMessage.Value.QueryAny(cmd))) {
                is ServerResponse.CommandOrQueryResponse ->
                    return@withContext r.response as R
                is ServerResponse.Error ->
                    throw RaftError(r)
                else ->
                    throw RaftError(ServerResponse.Error.BadResponse(r))
            }
        }
    }

    suspend fun leaveGroup() {
        return withContext(this.server.extraCoroContexts()) {
            if (this@Raft.isLeader()) {
                return@withContext
            }

            val leader = this@Raft.server.members.leader ?: throw RaftError(ServerResponse.Error.NotRunning)

            when (val r = leader.callServer(SyncMessage.Value.RemoveFollower(Server.getPID()))) {
                is ServerResponse.Ok ->
                    return@withContext
                is ServerResponse.Error ->
                    throw RaftError(r)
                else ->
                    throw RaftError(ServerResponse.Error.BadResponse(r))
            }
        }
    }

    companion object {
        suspend fun <C : Serializable, Q : Serializable, R : Serializable, D : Data<C, Q, R>>
                newGroup(config: Config, name: String, ctor: () -> D): Raft<C, Q, R, D> {
            val server = Server.newGroup(config, ctor)
            Registry.spawn(name, server)
            return Raft(server, name)
        }

        suspend fun <C : Serializable, Q : Serializable, R : Serializable, D : Data<C, Q, R>>
                joinGroup(knownMembers: List<Pair<String, Long>>, name: String): Raft<C, Q, R, D> {
            val remotePIDs = arrayListOf<PID>()
            knownMembers.forEach { (name, nodeId) ->
                Registry.lookupNamed(name, nodeId)?.let { remotePIDs.add(it) }
            }
            val server = Server.joinGroup<C, Q, R, D>(remotePIDs)
            Registry.spawn(server)
            return Raft(server, name)
        }


        @OptIn(ExperimentalTime::class)
        fun baseConfig(): Config {
            return Config(
                    1000.milliseconds,
                    1500.milliseconds,
                    100
            )
        }
    }
}