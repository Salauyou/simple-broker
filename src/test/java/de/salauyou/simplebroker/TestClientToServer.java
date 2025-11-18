package de.salauyou.simplebroker;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestClientToServer {

  private static final Logger logger = LoggerFactory.getLogger(TestClientToServer.class);
  final String host = "localhost";
  final int port = 7000;

  @Test
  void clientsSendMessagesEachOther() throws ExecutionException, InterruptedException {
    var q1 = new LinkedBlockingQueue<>(List.of("A", "B", "C", "D"));
    var q2 = new LinkedBlockingQueue<String>();

    var topic1 = "Topic 1";
    var topic2 = "Topic 2";
    var server = new Server(port);
    server.start();

    var client1 = new Client("Client1", host, port);
    var client2 = new Client("Client2", host, port);
    client1.start();
    client2.start();

    // transfer from q1 to q2 via topic 2
    client2.subscribe(topic2, message -> {
      assertEquals(topic2, message.getTopic());
      q2.add(new String(message.getBody(), StandardCharsets.UTF_8));
    }).get();

    for (var it : q1) {
      client1.send(topic2, it.getBytes(StandardCharsets.UTF_8), false);
      Thread.sleep(100);  // slow production
    }
    // send additional sync message and wait consumption
    client1.send(topic2, "sync 1".getBytes(StandardCharsets.UTF_8), true).get();

    assertEquals(List.of("A", "B", "C", "D", "sync 1"), q2.stream().toList());

    // transfer back from q2 to q1 via topic 1
    q1.clear();
    client1.subscribe(topic1, message -> {
      assertEquals(topic1, message.getTopic());
      q1.add(new String(message.getBody(), StandardCharsets.UTF_8));
      try {
        Thread.sleep(100);  // slow consumption
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }).get();

    for (var it : q2) {
      client2.send(topic1, it.getBytes(StandardCharsets.UTF_8), false);
    }
    // send additional sync message and wait consumption
    client2.send(topic1, "sync 2".getBytes(StandardCharsets.UTF_8), true).get();

    assertEquals(List.of("A", "B", "C", "D", "sync 1", "sync 2"), q1.stream().toList());

    logger.info("--------- stopping client1 -----------");
    client1.stop();

    // when client1 is stopped, it will not accept anything more
    for (var it : q2) {
      client2.send(topic1, it.getBytes(StandardCharsets.UTF_8), true).get();
    }

    assertEquals(List.of("A", "B", "C", "D", "sync 1", "sync 2"), q1.stream().toList());

    logger.info("--------- stopping client2 -----------");
    client2.stop();

    logger.info("--------- stopping server -----------");
    server.stop();
  }
}
