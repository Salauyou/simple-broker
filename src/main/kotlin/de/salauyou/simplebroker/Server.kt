package de.salauyou.de.salauyou.simplebroker

import de.salauyou.simplebroker.Message
import org.slf4j.LoggerFactory
import java.io.InputStream
import java.io.OutputStream
import java.net.ServerSocket
import java.net.Socket
import java.net.SocketAddress
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
    private val clientConnections = LinkedBlockingQueue<ClientConnection>()
    private val topicSubscriptions = ConcurrentHashMap<String, Queue<ClientConnection>>()
    private val pendingSyncRequests = ConcurrentHashMap<Long, Pair<ClientConnection, Queue<ClientConnection>>>()

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
                it.close()
            }
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
                val connection = ClientConnection(socket)
                clientConnections.add(connection)
                logger.info("Remote client connected to server: ${connection.remoteAddress}")
                clientConnectionThreadFactory.newThread {
                    connection.run()
                }.start()
            } ?: return
        }
    }

    private inner class ClientConnection(val socket: Socket) {
        val remoteAddress: SocketAddress = socket.remoteSocketAddress
        val input: InputStream = socket.getInputStream()
        val output: OutputStream = socket.getOutputStream()

        fun run() {
            while (true) {
                if (closed()) {
                    logger.info("Remote client $remoteAddress closed connection")
                    return
                }
                when (val mark = readMark(input)) {
                    CLOSED -> {
                        logger.info("Remote client $remoteAddress closed connection")
                        return
                    }
                    MESSAGE -> {
                        val message = readMessage(input)
                        val topic = message.getTopic()
                        logger.info("Received message from $remoteAddress to topic '$topic' (${message.getBody().size} bytes)")
                        topicSubscriptions[topic]?.let { clients ->
                            logger.info("Sending message to subscribed clients")
                            clients.forEach {
                                writeMessage(it.output, message)
                            }
                            logger.info("Message sent to subscribed clients")
                        } ?: logger.info("No clients subscribed to topic '$topic', message dropped")
                    }
                    SUBSCRIBE_REQUEST -> {
                        val request = readUtilityMessage(input)
                        logger.info("Received subscription request from $remoteAddress for topic '${request.topic}'")
                        topicSubscriptions.computeIfAbsent(request.topic) {
                            LinkedBlockingQueue()
                        }.add(this)
                        writeUtilityMessage(SUBSCRIBE_ACK, output, request.topic, 0L)
                        logger.info("Subscribed remote client $remoteAddress to topic '${request.topic}'")
                    }
                    SYNC_REQUEST -> {
                        val request = readUtilityMessage(input)
                        logger.info("Received sync request from $remoteAddress: $request")
                        val subscribedClients = topicSubscriptions[request.topic].orEmpty()
                        if (subscribedClients.isEmpty()) {
                            writeUtilityMessage(SYNC_RESPONSE, output, request.topic, request.id)
                            logger.info("No clients subscribed to topic ${request.topic}, sync response sent to remote client ${this.remoteAddress}")
                        } else {
                            subscribedClients.forEach {
                                pendingSyncRequests.computeIfAbsent(request.id) {
                                    this to LinkedBlockingQueue()
                                }.second.add(it)
                                writeUtilityMessage(SYNC_REQUEST, it.output, request.topic, request.id)
                                logger.info("Sync request for $request sent to remote client ${it.remoteAddress}")
                            }
                        }
                    }
                    SYNC_ACK -> {
                        val request = readUtilityMessage(input)
                        logger.info("Received sync ack from $remoteAddress: $request")
                        pendingSyncRequests[request.id]?.let {
                            val (initiator, awaitingClients) = it
                            awaitingClients.remove(this)
                            if (awaitingClients.isEmpty()) {
                                writeUtilityMessage(SYNC_RESPONSE, initiator.output, request.topic, request.id)
                                logger.info("Sent sync response for $request to remote client ${initiator.remoteAddress}")
                                pendingSyncRequests.remove(request.id)
                            }
                        }
                    }
                    else -> logger.error("Unexpected mark byte $mark")
                }
            }
        }

        fun close() {
            clientConnections.remove(this)
            topicSubscriptions.values.forEach { it.remove(this) }
            input.close()
            output.close()
            socket.close()
        }

        fun closed(): Boolean = socket.isClosed
    }

    internal class MessageImpl(private val topic: String, private val bytes: ByteArray): Message {
        override fun getTopic() = topic
        override fun getBody() = bytes
    }

    @OptIn(ExperimentalStdlibApi::class)
    internal class UtilityMessage(val topic: String, val id: Long) {
        override fun toString() = "topic='$topic', id=${id.toHexString()}"
    }

    companion object {
        private val logger = LoggerFactory.getLogger(Server::class.java)

        // mark bytes
        internal const val CLOSED: Byte = -1
        internal const val SUBSCRIBE_REQUEST: Byte = 1
        internal const val SUBSCRIBE_ACK: Byte = 2
        internal const val MESSAGE: Byte = 3
        internal const val SYNC_REQUEST: Byte = 4
        internal const val SYNC_ACK: Byte = 5
        internal const val SYNC_RESPONSE: Byte = 6

        internal fun readMark(input: InputStream): Byte {
            return try {
                input.read()
            } catch (e: SocketException) {
                CLOSED
            }.toByte()
        }

        internal fun writeMessage(output: OutputStream, message: Message) {
            synchronized(output) {
                val topicBytes = message.getTopic().encodeToByteArray()
                val messageBytes = message.getBody()
                val bytes = ByteBuffer.allocate(9 + topicBytes.size)
                    .put(MESSAGE)                // mark - 1b
                    .putInt(topicBytes.size)     // topic length - 4b
                    .put(topicBytes)             // topic
                    .putInt(messageBytes.size)   // message length - 4b
                    .array()
                output.write(bytes)              // message
                output.write(messageBytes)
                output.flush()
            }
        }

        internal fun readMessage(input: InputStream): Message {
            val topic = readTopic(input)
            val messageLength = ByteBuffer.wrap(input.readNBytes(4)).getInt()
            val bytes = input.readNBytes(messageLength)
            return MessageImpl(topic, bytes)
        }

        internal fun writeUtilityMessage(type: Byte, output: OutputStream, topic: String, id: Long) {
            synchronized(output) {
                val topicNameBytes = topic.encodeToByteArray()
                val bytes = ByteBuffer.allocate(13 + topicNameBytes.size)
                    .put(type)                     // mark - 1b
                    .putInt(topicNameBytes.size)   // topic length - 4b
                    .put(topicNameBytes)           // topic
                    .putLong(id)                   // id - 8b
                    .array()
                output.write(bytes)
                output.flush()
            }
        }

        internal fun readUtilityMessage(input: InputStream): UtilityMessage {
            val topic = readTopic(input)
            val id = ByteBuffer.wrap(input.readNBytes(8)).getLong()
            return UtilityMessage(topic, id)
        }

        private fun readTopic(input: InputStream): String {
            val topicNameLength = ByteBuffer.wrap(input.readNBytes(4)).getInt()
            return String(input.readNBytes(topicNameLength), StandardCharsets.UTF_8)
        }
    }
}