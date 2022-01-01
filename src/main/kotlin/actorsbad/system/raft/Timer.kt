package actorsbad.system.raft

import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.datetime.Clock
import kotlinx.datetime.Instant
import actorsbad.system.PID
import actorsbad.system.Process
import actorsbad.system.Registry
import java.io.Serializable
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

class Timer @OptIn(ExperimentalTime::class)
internal constructor(var delay: Duration, val msg: Serializable, val target: PID) {
    var startedAt: Instant? = null
        private set
    private var job: PID? = null
    var cancelled = false

    @OptIn(ExperimentalTime::class)
    suspend fun start() {
        this.job = Registry.spawn(object : Process() {
            override suspend fun mainFn() {
                var running = true
                while (true) {
                    withTimeoutOrNull(this@Timer.delay) {
                        if (receive() is Cancel) {
                            // can't break out of the withTimeout
                            running = false
                        }
                    }
                    if (!running || this@Timer.cancelled)
                        break
                    if (!this@Timer.target.send(this@Timer.msg)) {
                        // if we couldn't send, then the target is dead, we should die too
                        break
                    }
                }
            }
        })

        this.startedAt = Clock.System.now()
    }

    suspend fun cancel() {
        this.cancelled = true
        this.job?.send(Cancel)
    }
}

private object Cancel : Serializable