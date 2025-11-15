package de.salauyou.simplebroker
/**
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.concurrent.LinkedBlockingQueue
import kotlin.random.Random

class TestClientToServer {

    private val port = 7900 + Random.Default.nextInt(100)
    private val host = "localhost"
    private val topic = "Test topic"
    
    @Test
    fun `server starts, then client connects, then closes`() {
        val server = Server(port)
        server.start()
        val client = Client(host, port)
        client.start()
        client.stop()
        server.stop()
    }

    @Test
    fun `client requests sync for topic with no subscribers`() {
        val server = Server(port)
        server.start()
        val client = Client(host, port)
        client.start()
        client.sync(topic).get()
        client.stop()
        server.stop()
    }

    @Test
    fun `client requests sync for topic with self subscription`() {
        val server = Server(port)
        server.start()
        val client = Client(host, port)
        client.start()
        client.subscribe(topic, {}).get() // wait acknowledgement
        client.sync(topic).get()
        client.stop()
        server.stop()
    }

    @Test
    fun `client requests sync for topic with subscriber`() {
        val server = Server(port)
        server.start()
        val client1 = Client(host, port)
        client1.start()
        val client2 = Client(host, port)
        client2.start()
        client2.subscribe(topic, {}).get()
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
        val server = Server(port)
        server.start()

        val client1 = Client(host, port)
        val client2 = Client(host, port)
        client1.start()
        client2.start()

        // transfer from q1 to q2 via topic 2
        client2.subscribe(topic2) { message ->
            q2.add(String(message.getBody()))
        }.get()
        q1.forEach {
            client1.send(topic2, it.toByteArray())
            Thread.sleep(100)  // slow production
        }
        client1.sync(topic2).get()  // wait until all sent messages consumed
        assertEquals(q1.toList(), q2.toList())

        // transfer back from q2 to q1 via topic 1
        q1.clear()
        client1.subscribe(topic1) { message ->
            q1.add(String(message.getBody()))
            Thread.sleep(100)  // slow consumption
        }.get()
        q2.forEach {
            client2.send(topic1, it.toByteArray())
        }
        client2.sync(topic1).get()
        assertEquals(q2.toList(), q1.toList())

        client1.stop()

        // when client1 is stopped, it will not accept anything more
        q2.forEach {
            client2.send(topic1, it.toByteArray())
        }
        client2.sync(topic1).get()
        assertEquals(q2.toList(), q1.toList())

        client2.stop()
        server.stop()
    }
}*/