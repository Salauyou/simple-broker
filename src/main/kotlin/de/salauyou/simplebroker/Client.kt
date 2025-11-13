package de.salauyou.de.salauyou.simplebroker

import de.salauyou.de.salauyou.simplebroker.Server.Companion.CLOSED
import de.salauyou.de.salauyou.simplebroker.Server.Companion.MESSAGE
import de.salauyou.de.salauyou.simplebroker.Server.Companion.SUBSCRIBE_ACK
import de.salauyou.de.salauyou.simplebroker.Server.Companion.SUBSCRIBE_REQUEST
import de.salauyou.de.salauyou.simplebroker.Server.Companion.SYNC_ACK
import de.salauyou.de.salauyou.simplebroker.Server.Companion.SYNC_REQUEST
import de.salauyou.de.salauyou.simplebroker.Server.Companion.SYNC_RESPONSE
import de.salauyou.de.salauyou.simplebroker.Server.Companion.readMark
import de.salauyou.de.salauyou.simplebroker.Server.Companion.readMessage
import de.salauyou.de.salauyou.simplebroker.Server.Companion.readUtilityMessage
import de.salauyou.de.salauyou.simplebroker.Server.Companion.writeMessage
import de.salauyou.de.salauyou.simplebroker.Server.Companion.writeUtilityMessage
import de.salauyou.simplebroker.Message
import org.slf4j.LoggerFactory
import java.io.InputStream
import java.net.Socket
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.Future

class Client(private val host: String, private val port: Int) {

    @Volatile private var socket: Socket? = null
    private val receiverExecutor = Executors.newSingleThreadExecutor()
    private val topicConsumers = ConcurrentHashMap<String, (Message) -> Unit>()
    private val pendingSubscribeRequests = ConcurrentHashMap<String, CompletableFuture<Unit>>()
    private val pendingSyncRequests = ConcurrentHashMap<Long, CompletableFuture<Unit>>()

    @Synchronized
    fun start() {
        logger.info("Starting the client")
        socket = Socket(host, port).also {
            it.keepAlive = true
            receiverExecutor.submit {
                runServerConnection(it.getInputStream())
            }

        }
        logger.info("Started the client")
    }

    @Synchronized
    fun stop() {
        socket?.let {
            logger.info("Stopping the client")
            it.outputStream?.close()
            it.close()
            socket = null
            logger.info("Stopped the client")
        }
    }

    fun sendToTopic(topic: String, bytes: ByteArray) {
        logger.info("Sending message to topic: '$topic' (${bytes.size} bytes)")
        socket?.outputStream?.let {
            val message = Server.MessageImpl(topic, bytes)
            writeMessage(it, message)
            logger.info("Message sent")
        } ?: throw IllegalArgumentException("Client stopped")
    }

    /**
     * Subscribes this client to given topic. Returned [Future]
     * will complete when server acknowledges the subscription
     */
    fun subscribeTopic(topic: String, consumer: (Message) -> Unit): Future<Unit> {
        logger.info("Sending subscription request for topic '$topic'")
        socket?.outputStream?.let {
            topicConsumers[topic] = consumer
            val promise = CompletableFuture<Unit>()
            pendingSubscribeRequests[topic] = promise
            writeUtilityMessage(SUBSCRIBE_REQUEST, it, topic, 0L)
            logger.info("Subscription request for topic '$topic' sent")
            return promise
        } ?: throw IllegalArgumentException("Client stopped")
    }

    /**
     * Sends a sync signal for given topic. Returned [Future] will
     * complete when all consumers, subscribed to this topic,
     * acknowledge this signal, meaning that they have consumed all
     * messages sent by this client before
     */
    @OptIn(ExperimentalStdlibApi::class)
    fun sync(topic: String): Future<Unit> {
        logger.info("Sending sync request for topic '$topic'")
        socket?.outputStream?.let {
            val id = UUID.randomUUID().leastSignificantBits
            val promise = CompletableFuture<Unit>()
            pendingSyncRequests[id] = promise
            writeUtilityMessage(SYNC_REQUEST, it, topic, id)
            logger.info("Sync request for topic '$topic', id=${id.toHexString()} sent")
            return promise
        } ?: throw IllegalArgumentException("Client stopped")
    }

    private fun runServerConnection(inputStream: InputStream) {
        while (true) {
            if (socket?.isClosed != false) {
                return
            }
            when (val mark = readMark(inputStream)) {
                CLOSED -> {
                    logger.info("Server closed connection")
                    return
                }
                MESSAGE -> {
                    val message = readMessage(inputStream)
                    logger.info("Received message from topic '${message.getTopic()}' (${message.getBody().size} bytes)")
                    topicConsumers[message.getTopic()]?.invoke(message)
                }
                SUBSCRIBE_ACK -> {
                    val response = readUtilityMessage(inputStream)
                    logger.info("Received subscription ack for topic '${response.topic}'")
                    pendingSubscribeRequests.remove(response.topic)?.complete(Unit)
                }
                SYNC_REQUEST -> {
                    val request = readUtilityMessage(inputStream)
                    logger.info("Received sync request for $request")
                    socket?.outputStream?.let {
                        writeUtilityMessage(SYNC_ACK, it, request.topic, request.id)
                        logger.info("Acked sync request $request")
                    } ?: logger.info("Unable to ack: socket is closed")
                }
                SYNC_RESPONSE -> {
                    val response = readUtilityMessage(inputStream)
                    logger.info("Received sync response for $response")
                    pendingSyncRequests.remove(response.id)?.complete(Unit)
                }
                else -> logger.error("Unexpected mark byte $mark")
            }
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(Client::class.java)
    }
}