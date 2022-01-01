package actorsbad.system.messages

import actorsbad.system.PID
import java.io.Serializable

data class ProcessExitMessage(val exitedProcess: PID, val reason: Any?) : Serializable