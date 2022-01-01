package actorsbad.system

import java.io.Serializable

sealed class StateMachineMessage: Serializable {
    object Quit : StateMachineMessage()
}

sealed class StateMachineResponse<out State> {
    object Stop : StateMachineResponse<Nothing>()
    object SameState : StateMachineResponse<Nothing>()
    class NextState<State>(val state: State) : StateMachineResponse<State>()
    class NextStateWithEvents<State>(val state: State, val events: Collection<Any>) : StateMachineResponse<State>()
}

abstract class StateMachine<State>(initialState: State) : Process() {
    private var currentState: State = initialState
    private val nextEvents: ArrayDeque<Any> = ArrayDeque()

    private suspend fun nextEvent(): Any =
            this.nextEvents.removeFirstOrNull() ?: this.receive()

    override suspend fun mainFn() {
        while (true) {
            when (val msg = this.nextEvent()) {
                is StateMachineMessage.Quit -> break
                else -> {
                    when (val r = this.handleEvent(this.currentState, msg)) {
                        StateMachineResponse.SameState -> continue
                        is StateMachineResponse.NextState -> this.currentState = r.state
                        is StateMachineResponse.NextStateWithEvents -> {
                            this.currentState = r.state
                            r.events.reversed().forEach { this.nextEvents.addFirst(it) }
                            this.nextEvents.addAll(r.events)
                        }
                        StateMachineResponse.Stop -> break
                }
            }}
        }
    }

    abstract suspend fun handleEvent(state: State, message: Any): StateMachineResponse<State>
}