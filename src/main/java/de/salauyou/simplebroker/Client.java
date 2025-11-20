package de.salauyou.simplebroker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.StandardSocketOptions;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static de.salauyou.simplebroker.SocketConnection.*;

public class Client {

  private static final Logger logger = LoggerFactory.getLogger(Client.class);

  private final Executor receiverExecutor;
  private final Map<String, Consumer<? super Message>> topicConsumers = new ConcurrentHashMap<>();
  private final Map<Integer, CompletableFuture<Void>> pendingAcks = new ConcurrentHashMap<>();
  private final AtomicInteger idCounter = new AtomicInteger(0);   // will be reset during handshake

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
      logger.info("Client {} already started", clientId);
      return;
    }
    logger.info("Connecting the client {} to {}:{}", clientId, host, port);
    try {
      var socket = new Socket();
      socket.setKeepAlive(true);
      socket.setOption(StandardSocketOptions.TCP_NODELAY, true);
      socket.connect(new InetSocketAddress(host, port));
      connection = new Connection(socket);
      receiverExecutor.execute(() -> {
        try {
          connection.run();
        } catch (Exception e) {
          logger.error("Stopping " + clientId + " due to exception", e);
          stop();
        }
      });
      connection.doHandshake();
      logger.info("Started the client {}", clientId);
    } catch (Exception e) {
      logger.error("Could not start the client " + clientId, e);
      connection.close();
      throw (e instanceof RuntimeException ex) ? ex : new RuntimeException(e);
    }
  }

  public synchronized void stop() {
    if (connection == null) {
      logger.info("Client {} not started", clientId);
      return;
    }
    logger.info("Stopping client {}", clientId);
    connection.close();
    connection = null;
    topicConsumers.clear();
    pendingAcks.values().forEach(it ->
      it.completeExceptionally(new RuntimeException("Client stopped"))
    );
    pendingAcks.clear();
    logger.info("Stopped client {}", clientId);
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

    private final CompletableFuture<?> pendingHandshake = new CompletableFuture<Void>();

    private Connection(Socket socket) throws IOException {
      super(socket);
    }

    private void doHandshake() throws Exception {
      synchronized (output) {
        writeMessage(HANDSHAKE, idCounter.incrementAndGet(), clientId, null);
        logger.info("Client handshake request sent");
      }
      pendingHandshake.get(30, TimeUnit.SECONDS);  // wait response from server
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
            logger.info("Received handshake response, seq={}", msg.id());
            idCounter.set(msg.id() * 1_000_000);
            pendingHandshake.complete(null);
          }
          case SUBSCRIBE_ACK -> {
            var msg = readMessage(false);
            logger.info("Received subscription ack for topic={}", msg.topic());
            var promise = pendingAcks.remove(msg.id());
            if (promise != null) {
              promise.complete(null);
            }
          }
          case MESSAGE, SYNC_MESSAGE -> {
            var msg = readMessage(true);
            var sync = (mark == SYNC_MESSAGE);
            logger.info("Received {} id={} from topic={} ({} bytes)",
              (sync ? "sync message" : "message"), msg.getId(), msg.topic(), msg.body().length);
            var consumer = topicConsumers.get(msg.topic());
            if (consumer != null) {
              try {
                consumer.accept(msg);
              } catch (Exception e) {
                logger.error("Uncaught exception when consuming message", e);
              }
            }
            if (mark == SYNC_MESSAGE) {
              synchronized (connection.output) {
                writeMessage(MESSAGE_ACK, msg.id(), msg.topic(), null);
              }
              logger.info("Sent message ack id={}", msg.getId());
            }
          }
          case MESSAGE_ACK -> {
            var msg = readMessage(false);
            logger.info("Received message ack id={}", msg.getId());
            var promise = pendingAcks.remove(msg.id());
            if (promise != null) {
              promise.complete(null);
            }
          }
          default -> throw new IOException("Unexpected mark 0x" + Integer.toHexString(mark));
        }
      }
    }

    private CompletableFuture<?> subscribe(String topic, Consumer<? super Message> consumer) {
      topicConsumers.put(topic, consumer);
      var id = idCounter.incrementAndGet();
      var ackPromise = pendingAcks.computeIfAbsent(id, it -> new CompletableFuture<>());
      logger.info("Sending subscription request id={} for topic={}", id, topic);
      try {
        synchronized (connection.output) {
          writeMessage(SUBSCRIBE_REQUEST, id, topic, null);
        }
        logger.info("Subscription request sent");
      } catch (IOException e) {
        logger.warn("Could not send subscription request");
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
      logger.info("Sending {} id={} into topic={} ({} bytes)",
        (sync ? "sync message" : "message"), id, topic, body.length);
      try {
        synchronized (connection.output) {
          writeMessage((sync ? SYNC_MESSAGE : MESSAGE), id, topic, body);
        }
        logger.info("Message sent");
      } catch (IOException e) {
        logger.warn("Could not send message");
        pendingAcks.remove(id);
        throw new IOExceptionWrapper(e);
      }
      return ackPromise;
    }
  }
}