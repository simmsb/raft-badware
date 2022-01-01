package actorsbad.system

import java.io.Serializable

/**
 * A process ID, can be used to send messages to a process
 */
class PID internal constructor(val nodeID: Long, val processID: Long) : Serializable {
    suspend fun <T : Serializable> send(msg: T): Boolean {
        return Registry.send(this, msg)
    }

    override fun toString(): String {
        return "PID{$nodeID.$processID}"
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as PID

        if (nodeID != other.nodeID) return false
        if (processID != other.processID) return false

        return true
    }

    override fun hashCode(): Int {
        var result = nodeID.hashCode()
        result = 31 * result + processID.hashCode()
        return result
    }


}