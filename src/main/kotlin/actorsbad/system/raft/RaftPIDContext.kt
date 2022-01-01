package actorsbad.system.raft

import actorsbad.system.PID
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext

class RaftPIDContext(var pid: PID): AbstractCoroutineContextElement(RaftPIDContext) {
    companion object Key : CoroutineContext.Key<RaftPIDContext>
}