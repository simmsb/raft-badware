package actorsbad.system.socket.commands

import actorsbad.system.PID
import actorsbad.system.Process
import java.io.Serializable

sealed class Command: Serializable {
    object GetID : Command()
    data class Down<T: Serializable>(val pid: PID, val reason: T?): Command()
    data class Ping(val nodes: List<Pair<String, Int>>) : Command()
    data class SendName<T: Serializable>(val name: String, val msg: T): Command()
    data class SendPID<T: Serializable>(val pid: PID, val msg: T): Command()
    data class Spawn(val process: () -> Process): Command()
    data class LookupNamedProc(val name: String): Command()
}
