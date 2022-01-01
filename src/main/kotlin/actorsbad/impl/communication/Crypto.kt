package actorsbad.impl.communication

import java.io.IOException
import java.lang.RuntimeException
import java.nio.file.Paths
import java.security.KeyPairGenerator
import java.security.SecureRandom

object Crypto {
    val publicKeyPath = Paths.get(System.getenv("CRYPTO_STORE"), "pub.key")
    val privateKeyPath = Paths.get(System.getenv("CRYPTO_STORE"), "priv.key")

    val publicKey: ByteArray
    val privateKey: ByteArray

    init {
        val (pu, pr) = loadOrGenerateKeys()
        publicKey = pu
        privateKey = pr
    }

    private fun loadOrGenerateKeys(): Pair<ByteArray, ByteArray> {
        if (publicKeyPath.toFile().exists()) {
            try {
                val pub = publicKeyPath.toFile().readBytes()
                val priv = privateKeyPath.toFile().readBytes()
                return Pair(pub, priv)
            } catch (e: IOException) {
                throw RuntimeException("couldn't read keyfile")
            }
        } else {
            val keypairgen = KeyPairGenerator.getInstance("RSA")
            keypairgen.initialize(2048, SecureRandom())
            val pair = keypairgen.genKeyPair()
            publicKeyPath.toFile().writeBytes(pair.public.encoded)
            privateKeyPath.toFile().writeBytes(pair.private.encoded)
            return Pair(pair.public.encoded, pair.private.encoded)
        }
    }
}