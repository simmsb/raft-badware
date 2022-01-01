package actorsbad.system.raft

import java.io.Serializable

class Snapshot(
        val members: Members,
        val term: Long,
        val lastCommittedEntry: LogEntry,
        val config: Config,
        val data: Serializable,
        val commandTracker: CommandTracker,
): Serializable {
    companion object {
        fun fromInstallSnapShot(m: AsyncMessage.InstallSnapshot): Snapshot {
            return Snapshot(
                    m.members,
                    m.term,
                    m.lastCommittedEntry,
                    m.config,
                    m.data,
                    m.commandTracker
            )
        }

        suspend fun forLonelyLeader(config: Config, ctor: () -> Serializable): Snapshot {
            return Snapshot(
                    Members.newForLonelyLeader(),
                    0,
                    LogEntry(1, 0, LogEntry.LogValue.LeaderElected(Server.getPID(), arrayOf())),
                    config,
                    ctor(),
                    CommandTracker()
            )
        }
    }
}