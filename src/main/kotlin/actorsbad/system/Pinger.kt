package actorsbad.system

import kotlinx.coroutines.delay

class Pinger : Process() {
    override suspend fun mainFn() {
        while (true) {
            delay(1000)
            Registry.pingConnected()
        }
    }
}