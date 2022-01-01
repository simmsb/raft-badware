package actorsbad.system.raft

import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.io.Serializable

class SerializableArrayDeque<E : Serializable>
internal constructor(private var inner: ArrayDeque<E>) : Serializable {
    fun addFirst(e: E) {
        this.inner.addFirst(e)
    }

    fun removeLast(): E {
        return this.inner.removeLast()
    }

    private fun readObject(ois: ObjectInputStream) {
        @Suppress("UNCHECKED_CAST")
        this.inner = ArrayDeque(ois.readObject() as List<E>)
    }

    private fun writeObject(oos: ObjectOutputStream) {
        oos.writeObject(this.inner.toList())
    }
}

class CommandTracker(
        private val ids: SerializableArrayDeque<Long> = SerializableArrayDeque(ArrayDeque()),
        private val results: HashMap<Long, Serializable> = HashMap(),
) : Serializable {
    fun get(cmd: Long): Serializable? {
        return this.results[cmd]
    }

    fun put(cmd: Long, result: Serializable, maxSize: Int) {
        this.results[cmd] = result
        this.ids.addFirst(cmd)

        while (this.results.size > maxSize) {
            this.results.remove(this.ids.removeLast())
        }
    }
}
