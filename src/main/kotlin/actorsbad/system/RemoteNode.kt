package actorsbad.system

import java.io.Serializable

/**
 * A connection to a remote node.
 *
 * Note, when a connection is made, the remote node should add this node to it's remote node list too.
 */
interface RemoteNode {
    /**
     * @return the ID of the remote node
     */
    suspend fun id(): Long?

    /**
     * Send a message to a pid on a remote node
     * @param <T> The type of the message (the other side will see this as Object)
     * @param pid The PID to send to
     * @param msg The msg to send
     * @return if sending succeeded
     */
    suspend fun <T : Serializable> send(pid: PID, msg: T): Boolean

    /**
     * Send a message to a named process on a remote node
     * @param <T> The type of the message (the other side will see this as Object)
     * @param name The process name to send to
     * @param msg The msg to send
     * @return if sending succeeded
    </T> */
    suspend fun <T : Serializable> send(name: String, msg: T): Boolean

    /**
     * Spawn a process on a remote node
     * @param <T> The type of the process
     * @param p The process to spawn
     * @return The PID of the remote process
    </T> */
    suspend fun <T : Process> spawn(p: () -> T): PID?

    /**
     * Notify that a process has died
     */
    suspend fun <T: Serializable> down(pid: PID, reason: T?): Boolean

    suspend fun ping(nodes: List<Pair<String, Int>>): Boolean

    suspend fun lookupNamedProc(name: String): PID?

    fun getAddress(): Pair<String, Int>
}