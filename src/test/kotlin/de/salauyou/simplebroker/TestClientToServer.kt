package de.salauyou.simplebroker

import de.salauyou.de.salauyou.simplebroker.Client
import de.salauyou.de.salauyou.simplebroker.Server
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class TestClientToServer {

    @Test
    fun `server starts, then client connects, then closes`() {
        val server = Server(7000)
        server.start()
        val client = Client("localhost", 7000)
        client.start()
        client.subscribeTopic("Test topic", {}).get()
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
        val q1 = mutableListOf("A", "B", "C")
        val q2 = mutableListOf<String>()

        val topic1 = "Topic 1"
        val topic2 = "Topic 2"
        val server = Server(7000)
        server.start()

        val client1 = Client("localhost", 7000)
        val client2 = Client("localhost", 7000)

        client1.start()
        client1.subscribeTopic(topic1) { message ->
            Thread.sleep(100)  // slow consumption
            q1.add(String(message.getBody()))
        }.get()

        client2.start()
        client2.subscribeTopic(topic2) { message ->
            Thread.sleep(100)
            q2.add(String(message.getBody()))
        }.get()

        // transfer from q1 to q2 via topic
        q1.forEach {
            client1.sendToTopic(topic2, it.toByteArray())
        }
        client1.sync(topic2).get()  // wait until all sent messages consumed
        assertEquals(q1, q2)

        // transfer back from q2 to q1
        q1.clear()
        q2.forEach {
            client2.sendToTopic(topic1, it.toByteArray())
        }
        client2.sync(topic1).get()
        assertEquals(q2, q1)

        client1.stop()
        client2.stop()
        server.stop()
    }
}