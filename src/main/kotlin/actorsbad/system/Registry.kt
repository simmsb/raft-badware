package actorsbad.system

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.runBlocking
import actorsbad.system.messages.ProcessExitMessage
import actorsbad.system.messages.exitreasons.NodeDisconnected
import java.io.*
import java.net.InetAddress
import java.rmi.Remote
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicLong

data class MonitorRef(internal val id: Long) : Serializable {
    companion object {
        private var counter: Long = 0

        fun new(): MonitorRef {
            return MonitorRef(this.counter++)
        }
    }
}

private class LinkedProcessState {
    // monitor refs -> (node id, remote process id)
    val refToPath: HashMap<Long, Pair<Long, PID>> = hashMapOf()

    // remote node id -> (remote process id -> (local process id, monitor ref))
    val nodes: HashMap<Long, HashMap<PID, ArrayList<Pair<PID, Long>>>> = hashMapOf()

    // NOTE: remote node could be ourself

    fun addMonitor(remotePID: PID, monitoringPID: PID): MonitorRef {
        val ref = MonitorRef.new()
        this.refToPath[ref.id] = Pair(remotePID.nodeID, remotePID)
        this.nodes.computeIfAbsent(remotePID.nodeID) { hashMapOf() }
                .computeIfAbsent(remotePID) { arrayListOf() }
                .add(Pair(monitoringPID, ref.id))
        return ref
    }

    fun unMonitor(ref: MonitorRef) {
        val path = this.refToPath.remove(ref.id) ?: return
        val mapForNode = this.nodes[path.first] ?: return
        val a = mapForNode[path.second] ?: return
        a.retainAll { it.second != ref.id }
    }

    suspend fun notifyNodeDown(nodeID: Long) {
        for ((remotePID, targets) in this.nodes.remove(nodeID) ?: return) {
            for ((targetPID, ref) in targets) {
                targetPID.send(ProcessExitMessage(remotePID, NodeDisconnected(nodeID)))
                this.refToPath.remove(ref)
            }
        }
    }

    suspend fun notifyPIDDown(remotePID: PID, reason: Serializable?) {
        val mapForNode = this.nodes[remotePID.nodeID] ?: return
        for ((targetPID, ref) in mapForNode.remove(remotePID) ?: return) {
            targetPID.send(ProcessExitMessage(remotePID, reason))
            this.refToPath.remove(ref)
        }
        if (mapForNode.isEmpty()) {
            this.nodes.remove(remotePID.nodeID)
        }
    }
}

fun <T : Serializable> roundTripSerialize(t: T): T {
    val buf = ByteArrayOutputStream().use { bos ->
        val oos = ObjectOutputStream(bos)
        oos.writeObject(t)
        oos.flush()
        bos.toByteArray()
    }
    @Suppress("UNCHECKED_CAST")
    ByteArrayInputStream(buf).use { bis ->
        val ois = ObjectInputStream(bis)
        return ois.readObject() as T
    }
}

/**
 * A registry for processes, singleton
 */
object Registry {
    val localID: Long
    private val nextProcessID = AtomicLong(0)
    private var nextUniqueRef = AtomicLong(1)
    private val processMap = HashMap<Long, Process>()
    private val namedProcesses = HashMap<String, PID>()
    private val remoteNodes = HashMap<Long, RemoteNode>()
    private val linkedProcessState = LinkedProcessState()

    // bit hacky, TODO: make this better
    private var coroutineScope: CoroutineScope? = null

    fun init(scope: CoroutineScope) {
        this.coroutineScope = scope
    }

    suspend fun addRemoteNode(r: RemoteNode) {
        val rid = r.id() ?: return

        if (this.remoteNodes.containsKey(rid)) {
            return
        }

        this.remoteNodes[rid] = r
    }

    fun remoteNodes(): Set<Long> {
        return this.remoteNodes.keys
    }

    fun sendLocal(p: PID, msg: Any) {
        runBlocking {
            this@Registry.processMap[p.processID]?.send(msg)
        }
    }

    fun sendLocal(name: String, msg: Any) {
        val pid = this.namedProcesses[name] ?: return
        this.sendLocal(pid, msg)
    }

    suspend fun <T : Serializable> send(p: PID, msg: T): Boolean {
        return when {
            p.nodeID == this.localID -> {
                this.processMap[p.processID]?.send(roundTripSerialize(msg)) ?: return false
                true
            }
            this.remoteNodes.containsKey(p.nodeID) -> {
                this.remoteNodes[p.nodeID]?.send(p, msg) ?: return false
            }
            else -> {
                false
            }
        }
    }

    suspend fun <T : Serializable> send(name: String, msg: T): Boolean {
        return this.namedProcesses[name]?.send(roundTripSerialize(msg)) ?: return false
    }

    suspend fun <T : Serializable> send(name: String, nodeID: Long, msg: T): Boolean {
        return when {
            nodeID == this.localID -> {
                this.namedProcesses[name]?.send(roundTripSerialize(msg)) ?: return false
            }
            this.remoteNodes.containsKey(nodeID) -> {
                this.remoteNodes[nodeID]?.send(name, msg) ?: return false
            }
            else -> {
                false
            }
        }
    }

    fun lookupNamed(name: String): PID? {
        return this.namedProcesses[name]
    }

    suspend fun lookupNamed(name: String, nodeID: Long): PID? {
        return if (nodeID == this.localID) {
            this.lookupNamed(name)
        } else {
            this.remoteNodes[nodeID]?.lookupNamedProc(name)
        }
    }

    suspend fun pingConnected() {
        val nodes = this.remoteNodes.values.map { it.getAddress() }

        for (nodeId in this.remoteNodes.keys.toList()) {
            val pr = this.remoteNodes[nodeId]?.ping(nodes) ?: false
            if (!pr) {
                // currently we just discard the node if it dies
                // maybe we want to do something else though?
                this.remoteNodes.remove(nodeId)
                this.linkedProcessState.notifyNodeDown(nodeId)
            }
        }
    }

    private fun generateLocalPID(): PID {
        return PID(this.localID, this.nextProcessID.getAndIncrement())
    }

    suspend fun <T : Process> spawn(name: String, process: T): PID {
        val pid = this.spawn(process)
        this.namedProcesses[name] = pid
        return pid
    }

    suspend fun <T : Process> spawn(process: T): PID {
        val pid = this.generateLocalPID()
        this.processMap[pid.processID] = process
        process.start(this.coroutineScope!!, pid)
        // println("spawned proc: pid = $pid, process: $process")
        return pid
    }

    /**
     * Like `link` but only b will be notified of a exiting
     * @param child the child
     * @param parent the parent
     * @return if the link was successful
     */
    fun linkSingleDirection(child: PID, parent: PID): MonitorRef? {
        return when {
            child.nodeID == this.localID -> {
                this.linkedProcessState.addMonitor(child, parent)
            }
            this.remoteNodes.containsKey(child.nodeID) ->
                // only set the remote notify if the PID is local
                if (parent.nodeID == this.localID)
                    this.linkedProcessState.addMonitor(child, parent)
                else
                    null
            else -> null
        }
    }

    fun unMonitor(ref: MonitorRef) {
        this.linkedProcessState.unMonitor(ref)
    }

    suspend fun localProcessDied(deadPID: PID, reason: Serializable?) {
        this.linkedProcessState.notifyPIDDown(deadPID, reason)
        for (remote in this.remoteNodes.values.toList()) {
            remote.down(deadPID, reason)
        }
    }

    suspend fun remoteProcessDied(deadPID: PID, reason: Serializable?) {
        this.linkedProcessState.notifyPIDDown(deadPID, reason)
    }

    /**
     * Link two processes
     * When a linked process exits, a notification is sent to all processes linked with it.
     * The notification sent is [actorsbad.actors.distributed.messages.ProcessExitMessage]
     * The PIDs to be linked don't have to exist on this node, but must exist on a reachable node
     */
    fun link(a: PID, b: PID): Boolean {
        if (this.linkSingleDirection(a, b) == null) {
            return false
        }

        return this.linkSingleDirection(b, a) != null
    }

    private fun generateLocalID(): Long {
        val time = System.currentTimeMillis()
        val random = ThreadLocalRandom.current().nextLong()
        return time shl 20 or (random and 0xFFFFF)
    }

    fun generateUniqueRef(): Long {
        val id = this.nextUniqueRef.getAndIncrement()
        return this.localID shl 10 or id
    }

    private val hostName: String by lazy { InetAddress.getLocalHost().hostAddress }

    private val backendPort: Int by lazy {
        System.getenv("BACKEND_PORT")?.toIntOrNull() ?: throw Exception("BACKEND_PORT was not set or invalid")
    }

    fun getRemoteIdByAddress(addr: Pair<String, Int>): Long? {
        for (remote in this.remoteNodes) {
            if (remote.value.getAddress() == addr)
                return remote.key
        }

        return null
    }

    suspend fun connectToRemoteNodes(nodes: List<Pair<String, Int>>, cb: suspend (CoroutineScope, Pair<String, Int>) -> RemoteNode): List<RemoteNode> {
        val ourNodes = this.remoteNodes.values.map { it.getAddress() }.toSet()

        val ourself = Pair(this.hostName, this.backendPort)

        val connectedTo = arrayListOf<RemoteNode>()

        for (node in nodes) {
            if (node in ourNodes)
                continue

            if (node == ourself)
                continue

            println("We don't have $node in $ourNodes, connecting")

            val remote = cb(this.coroutineScope!!, node)
            this.addRemoteNode(remote)
            connectedTo.add(remote)
        }

        return connectedTo
    }

    init {
        this.localID = this.generateLocalID()
    }
}