package actorsbad.impl.communication

import kotlinx.coroutines.channels.Channel
import kotlinx.serialization.Serializable
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import actorsbad.impl.AuctioningNotification
import actorsbad.impl.AuctioningResponse
import java.nio.ByteBuffer
import java.security.KeyFactory
import java.security.PrivateKey
import java.security.PublicKey
import java.security.Signature
import java.security.spec.PKCS8EncodedKeySpec
import java.security.spec.X509EncodedKeySpec
import java.util.concurrent.ThreadLocalRandom
import javax.crypto.Cipher
import javax.crypto.KeyGenerator
import javax.crypto.SecretKey
import javax.crypto.spec.SecretKeySpec

private val HEX_CHARS = "0123456789ABCDEF".toCharArray()

fun ByteArray.toHex(): String {
    val result = StringBuffer()

    this.forEach {
        val octet = it.toInt()
        val firstIndex = (octet and 0xF0).ushr(4)
        val secondIndex = octet and 0x0F
        result.append(HEX_CHARS[firstIndex])
        result.append(HEX_CHARS[secondIndex])
    }

    return result.toString()
}

@Serializable
private class OpeningMessage(
        val publicKey: ByteArray,
        val challenge: Long,
)

@Serializable
private class ServerResponse(
        val challengeResponse: Long,
        val challenge: Long,
        val nonce: Long,
        val nonceSig: ByteArray,
)

@Serializable
private class ClientResponse(
        val challengeResponse: Long,
        val nonce: Long,
        val nonceSig: ByteArray,
)

@Serializable
private class Message(
        val content: String,
        val nonce: Long,
        val nonceSig: ByteArray,
)

// FLOW:
// 1. client encrypts an AES key with server's private key and sends it to the server
// 2. client sends it's (public key, challenge, nonce) encrypted with sym key
// 3. server sends back it's (challenge resp, challenge, nonce, signed nonce) encrypted with sym key -- this verifies the server
// 4. client replies with it's (challenge resp, nonce, signed nonce) encrypted with sym key -- this ensures that no replays are happening and verifies the client
//

@Serializable
private sealed class ResponseOrNotification {
    @Serializable
    class Response(val r: AuctioningResponse): ResponseOrNotification()
    @Serializable
    class Notif(val n: AuctioningNotification): ResponseOrNotification()
}

class AsyncEncryptedTransportServer(
        val channel: MessagedTransport,
        ourPrivKey: ByteArray,
        ourPubKey: ByteArray,
) {
    val ourPrivKey: PrivateKey
    val ourPubKey: PublicKey
    var symKey: SecretKey? = null
    var theirPubKey: PublicKey? = null

    init {
        val keyFactory = KeyFactory.getInstance("RSA")
        this.ourPrivKey = keyFactory.generatePrivate(PKCS8EncodedKeySpec(ourPrivKey))
        this.ourPubKey = keyFactory.generatePublic(X509EncodedKeySpec(ourPubKey))
    }

    suspend fun receiveMessage(): Commands? {
        val buf = this.readAndDecryptSym()
        val msg = Json.decodeFromString<Message>(buf.toString(Charsets.UTF_8))
        if (!this.verifyFromThem(msg.nonce, msg.nonceSig))
            return null
        return Json.decodeFromString(msg.content)
    }

    suspend fun sendResponse(msg: AuctioningResponse) {
        this.sendMessage(ResponseOrNotification.Response(msg))
    }

    suspend fun sendNotif(msg: AuctioningNotification) {
        this.sendMessage(ResponseOrNotification.Notif(msg))
    }

    private suspend fun sendMessage(msg: ResponseOrNotification) {
        val (nonce, signed) = this.makeAndSignNonce()
        val wrappedMessage = Message(Json.encodeToString(msg), nonce, signed)
        val encrypted = this.encryptSym(Json.encodeToString(wrappedMessage).toByteArray(Charsets.UTF_8))
        this.channel.writeMessage(ByteBuffer.wrap(encrypted))
    }

    private suspend fun readAndDecryptSym(): ByteArray {
        val buf = this.channel.readMessage()
        return this.decryptSym(buf)
    }

    private suspend fun readSymKey(): SecretKey {
        val buf = this.channel.readMessage()
        val cipher = Cipher.getInstance("RSA/ECB/PKCS1Padding")
        cipher.init(Cipher.PRIVATE_KEY, this.ourPrivKey)
        val key = cipher.doFinal(buf)
        return SecretKeySpec(key, "AES")
    }

    // Initiate the connection,
    suspend fun authenticate(): String? {
        this.symKey = this.readSymKey()
        val opening = this.receiveOpeningMessage()

        println("got opening")

        val keyFactory = KeyFactory.getInstance("RSA")
        this.theirPubKey = keyFactory.generatePublic(X509EncodedKeySpec(opening.publicKey))

        val ourChallenge = ThreadLocalRandom.current().nextLong()
        this.sendServerResponse(opening.challenge, ourChallenge)

        val clientResponse = this.receiveClientResponse()

        if (clientResponse.challengeResponse != ourChallenge)
            return null
        if (!this.verifyFromThem(clientResponse.nonce, clientResponse.nonceSig))
            return null

        return this.theirPubKey!!.encoded.toHex()
    }

    private fun longAsByteArray(long: Long): ByteArray {
        val buf = ByteBuffer.allocate(Long.SIZE_BYTES)
        buf.putLong(long)
        return buf.array()
    }

    private fun decryptToUs(message: ByteArray): ByteArray {
        val cipher = Cipher.getInstance("RSA")
        cipher.init(Cipher.DECRYPT_MODE, this.ourPrivKey)
        return cipher.doFinal(message)
    }

    private fun decryptSym(message: ByteArray): ByteArray {
        val cipher = Cipher.getInstance("AES")
        cipher.init(Cipher.DECRYPT_MODE, this.symKey)
        return cipher.doFinal(message)
    }

    private fun encryptSym(message: ByteArray): ByteArray {
        val cipher = Cipher.getInstance("AES")
        cipher.init(Cipher.ENCRYPT_MODE, this.symKey!!)
        return cipher.doFinal(message)
    }

    private fun signFromUs(nonce: Long): ByteArray {
        val sig = Signature.getInstance("SHA256withRSA")
        sig.initSign(this.ourPrivKey)
        sig.update(this.longAsByteArray(nonce))
        return sig.sign()
    }

    private fun verifyFromThem(nonce: Long, signedBuf: ByteArray): Boolean {
        val sig = Signature.getInstance("SHA256withRSA")
        sig.initVerify(this.theirPubKey)
        sig.update(this.longAsByteArray(nonce))

        return sig.verify(signedBuf)
    }

    private suspend fun receiveOpeningMessage(): OpeningMessage {
        val buf = this.readAndDecryptSym()
        return Json.decodeFromString<OpeningMessage>(buf.toString(Charsets.UTF_8))
    }

    private fun makeAndSignNonce(): Pair<Long, ByteArray> {
        val nonce = ThreadLocalRandom.current().nextLong()
        val signed = this.signFromUs(nonce)
        return Pair(nonce, signed)
    }

    private suspend fun sendServerResponse(challengeResponse: Long, ourChallenge: Long) {
        val (nonce, signed) = this.makeAndSignNonce()
        val serverResponse = ServerResponse(
                challengeResponse,
                ourChallenge,
                nonce,
                signed
        )
        val sre = Json.encodeToString(serverResponse).toByteArray(Charsets.UTF_8)
        val esre = this.encryptSym(sre)
        this.channel.writeMessage(ByteBuffer.wrap(esre))
    }

    private suspend fun receiveClientResponse(): ClientResponse {
        val buf = this.readAndDecryptSym()
        return Json.decodeFromString<ClientResponse>(buf.toString(Charsets.UTF_8))
    }

    fun close() {
        this.channel.close()
    }
}

class AsyncEncryptedTransportClient(
        val channel: MessagedTransport,
        ourPrivKey: ByteArray,
        val ourPubKeyA: ByteArray,
        theirPubKey: ByteArray,
) {
    private val notifications = Channel<AuctioningNotification>(Channel.UNLIMITED)
    private val responses = Channel<AuctioningResponse>(Channel.UNLIMITED)
    val ourPrivKey: PrivateKey
    val ourPubKey: PublicKey
    var theirPubKey: PublicKey
    val symKey: SecretKey

    init {
        val keyFactory = KeyFactory.getInstance("RSA")
        this.ourPrivKey = keyFactory.generatePrivate(PKCS8EncodedKeySpec(ourPrivKey))
        this.ourPubKey = keyFactory.generatePublic(X509EncodedKeySpec(this.ourPubKeyA))
        this.theirPubKey = keyFactory.generatePublic(X509EncodedKeySpec(theirPubKey))
        val symKeyGenerator = KeyGenerator.getInstance("AES")
        symKeyGenerator.init(128)
        this.symKey = symKeyGenerator.generateKey()
    }

    suspend fun runReceiver() {
        try {
            while (true) {
                when (val r = this.receiveMessage()) {
                    is ResponseOrNotification.Response ->
                        this.responses.send(r.r)

                    is ResponseOrNotification.Notif ->
                        this.notifications.send(r.n)

                    else ->
                        break
                }
            }
        } catch (e: Exception) {
            this.responses.close(e)
            this.notifications.close(e)
        }
    }

    private suspend fun receiveMessage(): ResponseOrNotification? {
        val buf = this.readAndDecryptSym()
        val msg = Json.decodeFromString<Message>(buf.toString(Charsets.UTF_8))
        if (!this.verifyFromThem(msg.nonce, msg.nonceSig))
            return null
        return Json.decodeFromString(msg.content)
    }

    suspend fun receiveResponse(): AuctioningResponse
        = this.responses.receive()

    suspend fun receiveNotification(): AuctioningNotification
        = this.notifications.receive()

    suspend fun sendMessage(msg: Commands) {
        val (nonce, signed) = this.makeAndSignNonce()
        val wrappedMessage = Message(Json.encodeToString(msg), nonce, signed)
        val encrypted = this.encryptSym(Json.encodeToString(wrappedMessage).toByteArray(Charsets.UTF_8))
        this.channel.writeMessage(ByteBuffer.wrap(encrypted))
    }

    private suspend fun readAndDecryptSym(): ByteArray {
        val buf = this.channel.readMessage()
        return this.decryptSym(buf)
    }

    private suspend fun sendSymKey() {
        val cipher = Cipher.getInstance("RSA/ECB/PKCS1Padding")
        cipher.init(Cipher.PUBLIC_KEY, this.theirPubKey)
        val buf = cipher.doFinal(this.symKey.encoded)
        this.channel.writeMessage(ByteBuffer.wrap(buf))
    }

    // Initiate the connection,
    suspend fun initiate(): Boolean {
        this.sendSymKey()

        val ourChallenge = ThreadLocalRandom.current().nextLong()
        this.sendOpeningMessage(ourChallenge)

        val serverResponse = this.receiveServerResponse()

        if (serverResponse.challengeResponse != ourChallenge)
            return false
        if (!this.verifyFromThem(serverResponse.nonce, serverResponse.nonceSig))
            return false

        this.sendClientResponse(serverResponse.challenge)

        return true
    }

    private fun longAsByteArray(long: Long): ByteArray {
        val buf = ByteBuffer.allocate(Long.SIZE_BYTES)
        buf.putLong(long)
        return buf.array()
    }

    private fun decryptSym(message: ByteArray): ByteArray {
        val cipher = Cipher.getInstance("AES")
        cipher.init(Cipher.DECRYPT_MODE, this.symKey)
        return cipher.doFinal(message)
    }

    private fun encryptSym(message: ByteArray): ByteArray {
        val cipher = Cipher.getInstance("AES")
        cipher.init(Cipher.ENCRYPT_MODE, this.symKey)
        return cipher.doFinal(message)
    }

    private fun signFromUs(nonce: Long): ByteArray {
        val sig = Signature.getInstance("SHA256withRSA")
        sig.initSign(this.ourPrivKey)
        sig.update(this.longAsByteArray(nonce))
        return sig.sign()
    }

    private fun verifyFromThem(nonce: Long, signedBuf: ByteArray): Boolean {
        val sig = Signature.getInstance("SHA256withRSA")
        sig.initVerify(this.theirPubKey)
        sig.update(this.longAsByteArray(nonce))

        return sig.verify(signedBuf)
    }

    private suspend fun sendOpeningMessage(ourChallenge: Long) {
        val openingMessage = OpeningMessage(
                this.ourPubKeyA,
                ourChallenge
        )
        val ome = Json.encodeToString(openingMessage).toByteArray(Charsets.UTF_8)
        val eome = this.encryptSym(ome)
        this.channel.writeMessage(ByteBuffer.wrap(eome))
    }

    private suspend fun sendClientResponse(challengeResponse: Long) {
        val (nonce, signed) = this.makeAndSignNonce()
        val clientResponse = ClientResponse(challengeResponse, nonce, signed)
        val cme = Json.encodeToString(clientResponse).toByteArray(Charsets.UTF_8)
        val ecme = this.encryptSym(cme)
        this.channel.writeMessage(ByteBuffer.wrap(ecme))
    }

    private fun makeAndSignNonce(): Pair<Long, ByteArray> {
        val nonce = ThreadLocalRandom.current().nextLong()
        val signed = this.signFromUs(nonce)
        return Pair(nonce, signed)
    }

    private suspend fun receiveServerResponse(): ServerResponse {
        val buf = this.readAndDecryptSym()
        return Json.decodeFromString<ServerResponse>(buf.toString(Charsets.UTF_8))
    }

    fun close() {
        this.channel.close()
    }
}
