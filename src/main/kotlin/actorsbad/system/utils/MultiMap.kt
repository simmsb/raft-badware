package actorsbad.system.utils

import java.util.*
import java.util.function.Function

// Give me HKTs aaaaaaaaaaaa
class MultiMap<K, V> {
    private val inner = HashMap<K, HashSet<V>>()

    operator fun get(key: K): HashSet<V> {
        return inner[key] ?: hashSetOf()
    }

    fun keys(): MutableSet<K> {
        return inner.keys
    }

    fun values(): Collection<HashSet<V>> {
        return inner.values
    }

    fun put(key: K, value: V): Boolean {
        inner.computeIfAbsent(key, Function { HashSet() }).add(value)
        return true
    }

    fun putAll(key: K, values: Collection<V>?): Boolean {
        inner.computeIfAbsent(key, Function { HashSet() }).addAll(values!!)
        return true
    }

    fun remove(key: K, value: V): Boolean {
        val l = inner[key]
        return l?.remove(value) ?: false
    }

    fun removeAll(key: K): Set<V> {
        return inner.remove(key)!!
    }
}