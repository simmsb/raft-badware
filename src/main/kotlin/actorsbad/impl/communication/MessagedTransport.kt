package actorsbad.impl.communication

import actorsbad.system.utils.AsyncSocketChannel
import java.io.IOException
import java.nio.ByteBuffer

class MessagedTransport(private val channel: AsyncSocketChannel) {
    suspend fun readMessage(): ByteArray {
        val lenBuf = ByteBuffer.allocate(Int.SIZE_BYTES)
        this.readNBytes(lenBuf, Int.SIZE_BYTES)
        val len = lenBuf.int
        val buf = ByteBuffer.allocate(len)
        this.readNBytes(buf, len)
        val bufOut = ByteArray(len)
        buf.get(bufOut, 0, len)
        return bufOut
    }

    suspend fun writeMessage(msg: ByteBuffer) {
        val len = msg.remaining()
        val lenBuf = ByteBuffer.allocate(Int.SIZE_BYTES)
        lenBuf.putInt(len)
        lenBuf.flip()
        this.channel.write(lenBuf)
        var written = 0
        while (written < len) {
            written += this.channel.write(msg)
        }
    }

    private suspend fun readNBytes(target: ByteBuffer, n: Int) {
        if (target.position() != 0)
            target.compact()

        while (target.position() < n) {
            val len = this.channel.read(target)
            if (!this.channel.channel.isOpen || len <= 0)
                throw IOException("socket closed while bytes remained to be read")
        }

        target.flip()
    }

    fun close() {
        this.channel.close()
    }
}