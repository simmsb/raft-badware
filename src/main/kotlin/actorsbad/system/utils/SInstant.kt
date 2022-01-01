package actorsbad.system.utils

import kotlinx.datetime.Instant
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.io.Serializable

@kotlinx.serialization.Serializable
class SInstant internal constructor(var ms: Long): Serializable {
    val inner
        get() = Instant.fromEpochMilliseconds(this.ms)

    private fun readObject(ois: ObjectInputStream) {
        @Suppress("UNCHECKED_CAST")
        this.ms = ois.readLong()
    }

    private fun writeObject(oos: ObjectOutputStream) {
        oos.writeLong(this.ms)
    }
}