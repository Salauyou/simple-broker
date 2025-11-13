package de.salauyou.simplebroker

import de.salauyou.de.salauyou.simplebroker.Client
import de.salauyou.de.salauyou.simplebroker.Server
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.concurrent.LinkedBlockingQueue

class TestClientToServer {

    @Test
    fun `server starts, then client connects, then closes`() {
        val server = Server(7000)
        server.start()
        val client = Client("localhost", 7000)
        client.start()
        client.stop()
        server.stop()
    }

    @Test
    fun `client requests sync for topic with no subscribers`() {
        val server = Server(7000)
        server.start()
        val client = Client("localhost", 7000)
        client.start()
        client.sync("Test topic").get()
        client.stop()
        server.stop()
    }

    @Test
    fun `client requests sync for topic with self subscription`() {
        val topic = "Test topic"
        val server = Server(7000)
        server.start()
        val client = Client("localhost", 7000)
        client.start()
        client.subscribeTopic(topic, {}).get() // wait acknowledgement
        client.sync(topic).get()
        client.stop()
        server.stop()
    }

    @Test
    fun `client requests sync for topic with subscriber`() {
        val topic = "Test topic"
        val server = Server(7000)
        server.start()
        val client1 = Client("localhost", 7000)
        client1.start()
        val client2 = Client("localhost", 7000)
        client2.start()
        client2.subscribeTopic(topic, {}).get()
        client1.sync(topic).get()
        client1.stop()
        client2.stop()
        server.stop()
    }

    @Test
    fun `clients sent messages to each other`() {
        val q1 = LinkedBlockingQueue(listOf("A", "B", "C", "D"))
        val q2 = LinkedBlockingQueue<String>()

        val topic1 = "Topic 1"
        val topic2 = "Topic 2"
        val server = Server(7000)
        server.start()

        val client1 = Client("localhost", 7000)
        val client2 = Client("localhost", 7000)
        client1.start()
        client2.start()

        // transfer from q1 to q2 via topic 2
        client2.subscribeTopic(topic2) { message ->
            q2.add(String(message.getBody()))
        }.get()
        q1.forEach {
            client1.sendToTopic(topic2, it.toByteArray())
            Thread.sleep(100)  // slow production
        }
        client1.sync(topic2).get()  // wait until all sent messages consumed
        assertEquals(q1.toList(), q2.toList())

        // transfer back from q2 to q1 via topic 1
        q1.clear()
        client1.subscribeTopic(topic1) { message ->
            q1.add(String(message.getBody()))
            Thread.sleep(100)  // slow consumption
        }.get()
        q2.forEach {
            client2.sendToTopic(topic1, it.toByteArray())
        }
        client2.sync(topic1).get()
        assertEquals(q2.toList(), q1.toList())

        client1.stop()
        client2.stop()
        server.stop()
    }
}