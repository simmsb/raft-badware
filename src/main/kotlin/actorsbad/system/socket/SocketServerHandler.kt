package actorsbad.system.socket

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.collect
import actorsbad.system.Process
import actorsbad.system.Registry
import actorsbad.system.socket.commands.Command
import actorsbad.system.socket.commands.Command.*
import java.io.EOFException
import java.io.Serializable
import kotlin.coroutines.coroutineContext

object NameDoesntExist: Serializable

class SocketServerHandler(private val sock: SocketMultiplexer): Process() {
    override suspend fun mainFn() {
        try {
            this.sock.fromInitiator().collect {
                this.sock.sendToInitiator(this.handle(it as Command))
            }
        } catch (e: EOFException) {
            // if the socket closes, just stop
        }
    }

    private suspend fun handle(cmd: Command): Serializable {
        when (cmd) {
            is GetID ->
                return Registry.localID

            is SendPID<*> ->
                return Registry.send(cmd.pid, cmd.msg)

            is SendName<*> ->
                return Registry.send(cmd.name, cmd.msg)

            is Spawn ->
                return Registry.spawn(cmd.process())

            is Ping -> {
                try {
                    Registry.connectToRemoteNodes(cmd.nodes) { scope, (addr, port) ->
                        SocketNode.connect(scope, addr, port)
                    }
                } catch (e: Exception) {
                    println("Sharing nodes failed with: $e")
                }
                return true
            }

            is Down<*> -> {
                Registry.remoteProcessDied(cmd.pid, cmd.reason)
                return true
            }

            is LookupNamedProc ->
                return Registry.lookupNamed(cmd.name) ?: NameDoesntExist
        }
    }
}