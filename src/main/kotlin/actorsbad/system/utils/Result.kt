package actorsbad.system.utils

import java.io.Serializable

sealed class Result<out T : Serializable, out E : Serializable>: Serializable {
    data class Ok<out T : Serializable>(val ok: T): Result<T, Nothing>()
    data class Err<out E: Serializable>(val err: E): Result<Nothing, E>()
}