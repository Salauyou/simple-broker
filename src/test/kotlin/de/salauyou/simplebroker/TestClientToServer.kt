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
        client.subscribeTopic("Test topic") {}
        Thread.sleep(100)
        server.stop()
        client.stop()
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
            q1.add(String(message.getBody()))
        }
        client2.start()
        client2.subscribeTopic(topic2) { message ->
            q2.add(String(message.getBody()))
        }

        // transfer from q1 to q2 via topic
        q1.forEach {
            client1.sendToTopic(topic2, it.toByteArray())
        }
        Thread.sleep(100)
        assertEquals(q1, q2)

        // transfer back from q2 to q1
        q1.clear()
        q2.forEach {
            client2.sendToTopic(topic1, it.toByteArray())
        }
        Thread.sleep(100)
        assertEquals(q2, q1)

        client1.stop()
        client2.stop()
        server.stop()
    }
}