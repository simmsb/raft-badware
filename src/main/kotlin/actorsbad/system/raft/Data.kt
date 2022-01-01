package actorsbad.system.raft

import java.io.Serializable

interface Data<C: Serializable, Q: Serializable, R: Serializable>: Serializable {
    fun command(command: C): R
    fun query(query: Q): R
}