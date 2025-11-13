package de.salauyou.de.salauyou.simplebroker

import de.salauyou.de.salauyou.simplebroker.Server.Companion.CLOSED
import de.salauyou.de.salauyou.simplebroker.Server.Companion.MESSAGE
import de.salauyou.de.salauyou.simplebroker.Server.Companion.readMark
import de.salauyou.de.salauyou.simplebroker.Server.Companion.readMessage
import de.salauyou.de.salauyou.simplebroker.Server.Companion.writeMessage
import de.salauyou.de.salauyou.simplebroker.Server.Companion.writeTopicSubscribe
import de.salauyou.simplebroker.Message
import org.slf4j.LoggerFactory
import java.io.InputStream
import java.net.Socket
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors

class Client(private val host: String, private val port: Int) {

    @Volatile private var socket: Socket? = null
    private val receiverExecutor = Executors.newSingleThreadExecutor()
    private val topicConsumers = ConcurrentHashMap<String, (Message) -> Unit>()

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

    fun subscribeTopic(topic: String, consumer: (Message) -> Unit) {
        logger.info("Subscribing to topic '$topic'")
        socket?.outputStream?.let {
            topicConsumers[topic] = consumer
            writeTopicSubscribe(it, topic)
            logger.info("Subscribed to topic '$topic'")
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
                else -> logger.error("Unexpected mark byte $mark")
            }
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(Client::class.java)
    }
}