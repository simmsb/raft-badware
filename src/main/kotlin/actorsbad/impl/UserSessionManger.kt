package actorsbad.impl

import actorsbad.system.PID
import actorsbad.system.Process
import actorsbad.system.Registry
import actorsbad.system.messages.ProcessExitMessage
import actorsbad.system.utils.MultiMap
import java.io.Serializable

sealed class UserSessionManagerMessage : Serializable {
    class AddMonitor(val from: PID, val userID: Long) : UserSessionManagerMessage()
    class NotifyUser(val userID: Long, val message: AuctioningNotification) : UserSessionManagerMessage()
    class GetSessions(val from: PID) : UserSessionManagerMessage()
}

sealed class UserSessionManagerResponse : Serializable {
    class Sessions(val users: List<Long>): UserSessionManagerResponse()
}
class UserSessionManger : Process() {
    private val pidMap: HashMap<PID, Long> = hashMapOf()
    private val userMap: MultiMap<Long, PID> = MultiMap()

    override suspend fun mainFn() {
        while (true) {
            when (val r = this.receive()) {
                is UserSessionManagerMessage.AddMonitor -> {
                    Registry.linkSingleDirection(r.from, this.self())
                    this.pidMap[r.from] = r.userID
                    this.userMap.put(r.userID, r.from)
                }

                is UserSessionManagerMessage.NotifyUser -> {
                    this.userMap[r.userID].forEach {
                        it.send(r.message)
                    }
                }

                is UserSessionManagerMessage.GetSessions -> {
                    val users = this.userMap.keys().toList()
                    r.from.send(UserSessionManagerResponse.Sessions(users))
                }

                is ProcessExitMessage -> {
                    val uid = this.pidMap.remove(r.exitedProcess) ?: continue
                    this.userMap.remove(uid, r.exitedProcess)
                }
            }
        }
    }

    companion object {
        suspend fun addMonitor(userID: Long) {
            val msg = UserSessionManagerMessage.AddMonitor(Process.self(), userID)
            Registry.send("userSessionManager", msg)
        }

        fun notifyUser(userID: Long, message: AuctioningNotification) {
            val msg = UserSessionManagerMessage.NotifyUser(userID, message)
            Registry.sendLocal("userSessionManager", msg)
        }

        suspend fun getSessions(): List<Long> {
            val msg = UserSessionManagerMessage.GetSessions(Process.self())
            Registry.send("userSessionManager", msg)
            val r = Process.receive()
            return (r as UserSessionManagerResponse.Sessions).users
        }

        suspend fun spawn() {
            Registry.spawn("userSessionManager", UserSessionManger())
        }
    }
}