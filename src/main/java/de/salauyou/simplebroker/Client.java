package de.salauyou.simplebroker;

import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.logging.Logger;

import static de.salauyou.simplebroker.SocketConnection.*;
import static java.util.logging.Level.*;

public class Client {

  private static final Logger logger = Logger.getLogger(Client.class.getCanonicalName());

  private final Executor receiverExecutor;
  private final Map<String, Consumer<? super Message>> topicConsumers = new ConcurrentHashMap<>();
  private final Map<Integer, CompletableFuture<Void>> pendingAcks = new ConcurrentHashMap<>();
  private final AtomicInteger idCounter = new AtomicInteger(hashCode());   // will be reinitialized on handshake

  private final String clientId;
  private final String host;
  private final int port;

  private volatile Connection connection;

  public Client(String clientId, String host, int port) {
    checkLength(clientId.getBytes(StandardCharsets.UTF_8));
    this.host = host;
    this.port = port;
    this.clientId = clientId;
    receiverExecutor = singleThreadExecutor("broker-conn-" + clientId);
  }

  public synchronized void start() {
    if (connection != null) {
      logger.log(INFO, "Client {0} already started", clientId);
      return;
    }
    logger.log(INFO, "Connecting the client {0} to {1}:{2}", new Object[]{clientId, host, port});
    try {
      var socket = new Socket(host, port);
      socket.setKeepAlive(true);
      var conn = new Connection(socket);
      synchronized (conn.output) {
        conn.writeMessage(HANDSHAKE, idCounter.incrementAndGet(), clientId, null);
        logger.info("Client handshake request sent");
      }
      receiverExecutor.execute(() -> {
        try {
          conn.run();
        } catch (Exception e) {
          logger.log(SEVERE, "Stopping " + clientId + " due to exception", e);
          stop();
        }
      });
      this.connection = conn;
      logger.info("Started the client");
    } catch (IOException e) {
      throw new IOExceptionWrapper(e);
    }
  }

  public synchronized void stop() {
    if (connection == null) {
      logger.log(INFO, "Client {0} not started", clientId);
      return;
    }
    logger.log(INFO, "Stopping client {0}", clientId);
    connection.close();
    connection = null;
    topicConsumers.clear();
    pendingAcks.values().forEach(it ->
      it.completeExceptionally(new RuntimeException("Client stopped"))
    );
    pendingAcks.clear();
    logger.log(INFO, "Stopped client {0}", clientId);
  }

  /**
   * Subscribes this client to given topic. Returned promise will complete
   * after server acknowledges the subscription
   */
  public CompletableFuture<?> subscribe(String topic, Consumer<? super Message> consumer) {
    return requireConnection().subscribe(topic, consumer);
  }

  /**
   * Sends message into topic. If sync=false, returned promise is already completed.
   * If sync=true, it will complete after all topic subscribers consume this message
   * normally or exceptionally
   */
  public CompletableFuture<?> send(String topic, byte[] body, boolean sync) {
    return requireConnection().send(topic, body, sync);
  }

  private Connection requireConnection() {
    var conn = connection;
    if (conn == null) {
      throw new IllegalArgumentException("Connection is closed");
    }
    return conn;
  }

  private class Connection extends SocketConnection {

    private Connection(Socket socket) throws IOException {
      super(socket);
    }

    private void run() throws IOException {
      while (true) {
        var mark = readMark();
        switch (mark) {
          case CLOSED -> {
            if (connection == null) {
              return;  // already stopped
            }
            logger.info("Server closed connection, client will be stopped");
            stop();
            return;
          }
          case HANDSHAKE -> {
            var msg = readMessage(false);
            logger.log(INFO, "Received handshake response, seq={0}", msg.id());
            idCounter.set(msg.id() << 20);
          }
          case SUBSCRIBE_ACK -> {
            var msg = readMessage(false);
            logger.log(INFO, "Received subscription ack for topic={0}", msg.topic());
            var promise = pendingAcks.remove(msg.id());
            if (promise != null) {
              promise.complete(null);
            }
          }
          case MESSAGE, SYNC_MESSAGE -> {
            var msg = readMessage(true);
            var sync = (mark == SYNC_MESSAGE);
            logger.log(INFO, "Received {0} id={1} from topic={2} ({3} bytes)",
              new Object[]{(sync ? "sync message" : "message"), msg.hexId(), msg.topic(), msg.body().length});
            var consumer = topicConsumers.get(msg.topic());
            if (consumer != null) {
              try {
                consumer.accept(msg);
              } catch (Exception e) {
                logger.log(SEVERE, "Uncaught exception when consuming message", e);
              }
            }
            if (mark == SYNC_MESSAGE) {
              synchronized (connection.output) {
                writeMessage(MESSAGE_ACK, msg.id(), msg.topic(), null);
              }
              logger.log(INFO, "Sent message ack id={0}", msg.hexId());
            }
          }
          case MESSAGE_ACK -> {
            var msg = readMessage(false);
            logger.log(INFO, "Received message ack id={0}", msg.hexId());
            var promise = pendingAcks.remove(msg.id());
            if (promise != null) {
              promise.complete(null);
            }
          }
          default -> throw new IOException("Unexpected mark " + mark);
        }
      }
    }

    private CompletableFuture<?> subscribe(String topic, Consumer<? super Message> consumer) {
      topicConsumers.put(topic, consumer);
      var id = idCounter.incrementAndGet();
      var ackPromise = pendingAcks.computeIfAbsent(id, it -> new CompletableFuture<>());
      logger.log(INFO, "Sending subscription request id={0} for topic={1}", new Object[]{Integer.toHexString(id), topic});
      try {
        synchronized (connection.output) {
          writeMessage(SUBSCRIBE_REQUEST, id, topic, null);
        }
        logger.info("Subscription request sent");
      } catch (IOException e) {
        logger.log(WARNING, "Could not send subscription request");
        pendingAcks.remove(id);
        throw new IOExceptionWrapper(e);
      }
      return ackPromise;
    }

    private CompletableFuture<?> send(String topic, byte[] body, boolean sync) {
      var id = idCounter.incrementAndGet();
      var ackPromise = sync
        ? pendingAcks.computeIfAbsent(id, it -> new CompletableFuture<>())
        : CompletableFuture.completedFuture(null);
      logger.log(INFO, "Sending {0} id={1} into topic={2} ({3} bytes)",
        new Object[]{(sync ? "sync message" : "message"), Integer.toHexString(id), topic, body.length});
      try {
        synchronized (connection.output) {
          writeMessage((sync ? SYNC_MESSAGE : MESSAGE), id, topic, body);
        }
        logger.info("Message sent");
      } catch (IOException e) {
        logger.log(SEVERE, "Could not send message");
        pendingAcks.remove(id);
        throw new IOExceptionWrapper(e);
      }
      return ackPromise;
    }
  }
}