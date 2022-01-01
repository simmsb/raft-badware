package actorsbad.system.raft

import java.io.Serializable
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

class Config @OptIn(ExperimentalTime::class) constructor(
        val heartbeatTimeout: Duration,
        val electionTimeout: Duration,
        val maxRetainedCommandResults: Int,
): Serializable