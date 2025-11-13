package de.salauyou.de.salauyou.simplebroker

import de.salauyou.simplebroker.Message
import org.slf4j.LoggerFactory
import java.io.InputStream
import java.io.OutputStream
import java.net.ServerSocket
import java.net.Socket
import java.net.SocketException
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue

class Server(private val port: Int) {

    @Volatile private var serverSocket: ServerSocket? = null
    private val socketListeningExecutor = Executors.newSingleThreadExecutor()
    private val clientConnectionThreadFactory = Executors.defaultThreadFactory()
    private val clientConnections = LinkedBlockingQueue<Socket>()
    private val topicSubscriptions = ConcurrentHashMap<String, Queue<OutputStream>>()

    @Synchronized
    fun start() {
        logger.info("Starting the server")
        serverSocket = ServerSocket(port)
        socketListeningExecutor.submit {
            listenClientConnections()
        }
        logger.info("Started the server")
    }

    @Synchronized
    fun stop() {
        serverSocket?.let {
            logger.info("Stopping the server")
            clientConnections.forEach {
                it.getOutputStream().close()
                it.close()
            }
            clientConnections.clear()
            topicSubscriptions.clear()
            it.close()
            serverSocket = null
            logger.info("Stopped the server")
        }
    }

    private fun listenClientConnections() {
        while (true) {
            serverSocket?.let {
                if (it.isClosed) {
                    return
                }
                val socket = it.accept()
                logger.info("Remote client connected to server: ${socket.remoteSocketAddress}")
                clientConnections.add(socket)
                clientConnectionThreadFactory.newThread {
                    runClientConnection(socket)
                }.start()
            } ?: return
        }
    }

    private fun runClientConnection(socket: Socket) {
        val remoteAddress = socket.remoteSocketAddress
        val inputStream = socket.getInputStream()
        val outputStream = socket.getOutputStream()
        while (true) {
            if (serverSocket?.isClosed != false) {
                return
            }
            if (socket.isClosed) {
                logger.info("Remote client $remoteAddress closed connection")
                return
            }
            when (val mark = readMark(inputStream)) {
                CLOSED -> {
                    logger.info("Remote client $remoteAddress closed connection")
                    return
                }
                TOPIC_SUBSCRIBE -> {
                    val topicNameLength = ByteBuffer.wrap(inputStream.readNBytes(4)).getInt()
                    val topic = String(inputStream.readNBytes(topicNameLength), StandardCharsets.UTF_8)
                    topicSubscriptions.computeIfAbsent(topic) {
                        LinkedBlockingQueue()
                    }.add(outputStream)
                    logger.info("Remote client $remoteAddress subscribed to topic '$topic'")
                }
                MESSAGE -> {
                    val message = readMessage(inputStream)
                    val topic = message.getTopic()
                    logger.info("Remote client $remoteAddress sent message to topic '$topic' (${message.getBody().size} bytes)")
                    topicSubscriptions[topic]?.let { clients ->
                        logger.info("Sending message to clients")
                        clients.forEach {
                            writeMessage(it, message)
                        }
                        logger.info("Message sent to clients")
                    } ?: logger.info("No clients subscribed to topic '$topic', message dropped")
                }
                else -> logger.error("Unexpected mark byte $mark")
            }
        }
    }

    internal class MessageImpl(private val topic: String, private val bytes: ByteArray): Message {
        override fun getTopic() = topic
        override fun getBody() = bytes
    }

    companion object {
        private val logger = LoggerFactory.getLogger(Server::class.java)
        internal const val CLOSED: Byte = -1
        internal const val TOPIC_SUBSCRIBE: Byte = 1
        internal const val MESSAGE: Byte = 2

        internal fun readMark(input: InputStream): Byte {
            return try {
                input.read()
            } catch (e: SocketException) {
                CLOSED
            }.toByte()
        }

        internal fun writeMessage(output: OutputStream, message: Message) {
            val topicBytes = message.getTopic().encodeToByteArray()
            val messageBytes = message.getBody()
            val bytes = ByteBuffer.allocate(9 + topicBytes.size)
                .put(MESSAGE)               // mark - 1b
                .putInt(topicBytes.size)    // topic length - 4b
                .put(topicBytes)            // topic
                .putInt(messageBytes.size)  // message length - 4b
                .array()
            output.write(bytes)             // message
            output.write(messageBytes)
            output.flush()
        }

        internal fun readMessage(input: InputStream): Message {
            val topicNameLength = ByteBuffer.wrap(input.readNBytes(4)).getInt()
            val topic = String(input.readNBytes(topicNameLength), StandardCharsets.UTF_8)
            val messageLength = ByteBuffer.wrap(input.readNBytes(4)).getInt()
            val bytes = input.readNBytes(messageLength)
            return MessageImpl(topic, bytes)
        }

        internal fun writeTopicSubscribe(output: OutputStream, topic: String) {
            val topicNameBytes = topic.encodeToByteArray()
            val bytes = ByteBuffer.allocate(5 + topicNameBytes.size)
                .put(TOPIC_SUBSCRIBE)          // mark - 1b
                .putInt(topicNameBytes.size)   // topic length - 4b
                .put(topicNameBytes)           // topic
                .array()
            output.write(bytes)
            output.flush()
        }
    }
}