package actorsbad.system

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import java.io.Serializable
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.coroutines.coroutineContext

/**
 * Represents a process with a messagebox
 */
abstract class Process {
    private val messagebox = Channel<Any>(Channel.UNLIMITED)
    private var thread: Job? = null
    private var pid: PID? = null

    abstract suspend fun mainFn()

    open fun extraCoroContexts(): CoroutineContext = EmptyCoroutineContext

    suspend fun start(scope: CoroutineScope, pid: PID) {
        this.pid = pid
        val ctx = CurrentProcessContext(this) + this.extraCoroContexts()
        this.thread = scope.launch(context = ctx) {
            this@Process.mainFnWrapper()
        }
    }

    private suspend fun mainFnWrapper() {
        var reason: Serializable? = null

        try {
            this.mainFn()
        } catch (e: Exception) {
            println("process exited with reason: $e")
            e.printStackTrace()
            reason = e
        }

        Registry.localProcessDied(this.self(), reason)
    }

    suspend fun receive(): Any {
        return this.messagebox.receive()
    }

    fun self(): PID {
        return this.pid!!
    }

    suspend fun send(msg: Any) {
        this.messagebox.send(msg)
    }

    companion object {
        suspend fun self(): PID {
            return currentProcess().pid ?: throw Exception("You're not in a process")
        }

        suspend fun receive(): Any {
            return currentProcess().receive()
        }

        suspend fun currentProcess(): Process {
            return coroutineContext[CurrentProcessContext]!!.process
        }
    }
}