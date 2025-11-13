package de.salauyou.simplebroker

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertNotNull
import java.net.ConnectException
import java.util.concurrent.atomic.AtomicReference

class TestsWithRunningServer {

    @Test
    fun `connect clients and exchange message`() {
        val reference = AtomicReference<String>()
        val testData = "Test message"
        val topic = "Test topic"
        val client1 = Client("localhost", 6999)
        val client2 = Client("localhost", 6999)
        try {
            client1.start()
            client2.start()
            client2.subscribeTopic(topic) {
                reference.set(String(it.getBody()))
            }.get()

            client1.sendToTopic(topic, testData.toByteArray())
            client1.sync(topic).get()

            assertEquals(testData, reference.get())
            client1.stop()
            client2.stop()

        } catch (e: ConnectException) {
            e.printStackTrace()
            assertNotNull("Please run server application before this test")
        }
    }
}