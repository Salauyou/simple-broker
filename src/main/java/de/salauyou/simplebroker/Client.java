package de.salauyou.simplebroker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static de.salauyou.simplebroker.SocketConnection.singleThreadExecutor;
import static de.salauyou.simplebroker.SocketConnection.IOExceptionWrapper;

public class Client {

  private static final Logger logger = LoggerFactory.getLogger(Client.class);
  private static final AtomicInteger COUNTER = new AtomicInteger();

  private final Executor receiverExecutor;
  private final Map<String, Consumer<? super Message>> topicConsumers = new ConcurrentHashMap<>();
  private final Map<String, CompletableFuture<Void>> pendingSubscribeRequests = new ConcurrentHashMap<>();
  private final Map<Long, CompletableFuture<Void>> pendingSyncRequests = new ConcurrentHashMap<>();

  private final String clientId;
  private final String host;
  private final int port;

  private volatile Connection connection;

  public Client(String clientId, String host, int port) {
    this.host = host;
    this.port = port;
    this.clientId = clientId;
    var threadName = "broker-conn-" + COUNTER.incrementAndGet() + "-" + clientId;
    receiverExecutor = singleThreadExecutor(threadName);
  }

  public synchronized void start() {
    if (connection != null) {
      logger.info("Client {} already connected", clientId);
      return;
    }
    logger.info("Connecting the client {} to {}:{}", clientId, host, port);
    try {
      var socket = new Socket(host, port);
      socket.setKeepAlive(true);
      connection = new Connection(socket);
      connection.writeClientId(clientId);
      receiverExecutor.execute(() -> {
        try {
          connection.run();
        } catch (Exception e) {
          logger.error("Stopping " + clientId + " due to exception", e);
          stop();
        }
      });
      logger.info("Connected the client");
    } catch (IOException e) {
      throw new IOExceptionWrapper(e);
    }
  }

  public synchronized void stop() {
    if (connection == null) {
      logger.info("Client {} already stopped", clientId);
      return;
    }
    connection.close();
    connection = null;
    topicConsumers.clear();
    pendingSyncRequests.clear();
    pendingSubscribeRequests.clear();
    logger.info("Stopped the client");
  }

  /**
   * Sends data into topic. For delivery control, use [sync] method
   */
  public void send(String topic, byte[] bytes) {
    requireConnection().sendToTopic(topic, bytes);
  }

  /**
   * Subscribes this client to given topic. Returned [Future]
   * will complete when server acknowledges the subscription
   */
  public CompletableFuture<?> subscribe(String topic, Consumer<? super Message> consumer) {
    return requireConnection().subscribeToTopic(topic, consumer);
  }

  /**
   * Sends a sync signal into given topic. Returned [Future] will
   * complete when all consumers, subscribed to this topic,
   * acknowledge this signal, meaning that they have consumed all
   * previous messages sent by this client
   */
  public CompletableFuture<?> sync(String topic) {
    return requireConnection().sync(topic);
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

    protected void run() throws IOException {
      while (true) {
        var mark = readMark();
        switch (mark) {
          case CLOSED -> {
            if (connection == null) {
              return; // already stopped
            }
            logger.info("Server closed connection, client will be stopped");
            stop();
            return;
          }
          case SUBSCRIBE_ACK -> {
            var response = readUtilityMessage();
            logger.info("Received subscription ack for topic={}", response.topic());
            var promise = pendingSubscribeRequests.remove(response.topic());
            if (promise != null) {
              promise.complete(null);
            }
          }
          case MESSAGE -> {
            var message = readMessage();
            logger.info("Received message from topic={} ({} bytes)", message.getTopic(), message.getBody().length);
            var consumer = topicConsumers.get(message.getTopic());
            if (consumer != null) {
              try {
                consumer.accept(message);
              } catch (Exception e) {
                logger.error("Uncaught exception when consuming message", e);
              }
            }
          }
          case SYNC_REQUEST -> {
            var request = readUtilityMessage();
            logger.info("Received sync request for {}", request);
            writeUtilityMessage(SYNC_ACK, request.topic(), request.id());
            logger.info("Acked sync request {}", request);
          }
          case SYNC_ACK -> {
            var response = readUtilityMessage();
            logger.info("Received sync ack for {}", response);
            var promise = pendingSyncRequests.remove(response.id());
            if (promise != null) {
              promise.complete(null);
            }
          }
          default -> throw new IOException("Unexpected mark byte " + mark);
        }
      }
    }

    void sendToTopic(String topic, byte[] bytes) {
      logger.info("Sending message to topic={} ({} bytes)", topic, bytes.length);
      var message = new MessageImpl(topic, bytes);
      try {
        writeMessage(message);
        logger.info("Message sent");
      } catch (IOException e) {
        logger.warn("Could not send message");
        throw new IOExceptionWrapper(e);
      }
    }

    CompletableFuture<?> subscribeToTopic(String topic, Consumer<? super Message> consumer) {
      topicConsumers.put(topic, consumer);
      logger.info("Sending subscription request for topic={}", topic);
      var promise = pendingSubscribeRequests.computeIfAbsent(topic, it -> new CompletableFuture<>());
      try {
        writeUtilityMessage(SUBSCRIBE_REQUEST, topic, 0L);
        logger.info("Sent subscription request for topic={}", topic);
      } catch (IOException e) {
        logger.warn("Could not send subscription request for topic={}", topic);
        promise.completeExceptionally(e);
        pendingSubscribeRequests.remove(topic);
        throw new IOExceptionWrapper(e);
      }
      return promise;
    }

    CompletableFuture<?> sync(String topic) {
      logger.info("Sending sync request for topic={}", topic);
      var id = UUID.randomUUID().getLeastSignificantBits();
      var promise = pendingSyncRequests.computeIfAbsent(id, it -> new CompletableFuture<>());
      try {
        writeUtilityMessage(SYNC_REQUEST, topic, id);
        logger.info("Sent sync request for topic={}, id={}", topic, Long.toHexString(id));
      } catch (IOException e) {
        logger.warn("Could not sent sync request for topic={}, id={}", topic, Long.toHexString(id));
        promise.completeExceptionally(e);
        pendingSyncRequests.remove(id);
        throw new IOExceptionWrapper(e);
      }
      return promise;
    }
  }
}