package actorsbad.system

import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext

class CurrentProcessContext(val process: Process): AbstractCoroutineContextElement(CurrentProcessContext) {
    companion object Key : CoroutineContext.Key<CurrentProcessContext>
}
