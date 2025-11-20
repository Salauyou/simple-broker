package de.salauyou.simplebroker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.util.Collections;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static de.salauyou.simplebroker.SocketConnection.IOExceptionWrapper;
import static de.salauyou.simplebroker.SocketConnection.singleThreadExecutor;

public class Server {

  private static final Logger logger = LoggerFactory.getLogger("server");

  private final int port;
  private final Executor clientConnectionListeningExecutor = singleThreadExecutor("server");
  private final ThreadFactory clientConnectionThreadFactory = Executors.defaultThreadFactory();
  private final AtomicInteger clientConnectionCounter = new AtomicInteger();
  private final Queue<ClientConnection> clientConnections = new ConcurrentLinkedQueue<>();
  private final Map<String, Map<ClientConnection, Boolean>> topicSubscriptions = new ConcurrentHashMap<>();
  private final Map<Integer, PendingAck> pendingAcks = new ConcurrentHashMap<>();

  private volatile ServerSocket serverSocket;

  public Server(int port) {
    this.port = port;
  }

  public synchronized void start() {
    if (serverSocket != null) {
      logger.debug("Already started");
    }
    logger.info("Starting the server at port {}", port);
    try {
      serverSocket = new ServerSocket(port);
      clientConnectionListeningExecutor.execute(() -> {
        try {
          listenClientConnections();
        } catch (Exception e) {
          logger.error("Stopping server due to exception", e);
          stop();
        }
      });
      logger.info("Started the server");
    } catch (IOException e) {
      throw new IOExceptionWrapper(e);
    }
  }

  public synchronized void stop() {
    if (serverSocket == null) {
      logger.debug("Not started");
    }
    logger.info("Stopping the server");
    clientConnections.forEach(ClientConnection::close);
    try {
      serverSocket.close();
    } catch (IOException ignored) {}
    topicSubscriptions.clear();
    pendingAcks.clear();
    serverSocket = null;
    logger.info("Stopped the server");
  }

  private void listenClientConnections() {
    while (true) {
      try {
        var socket = serverSocket.accept();
        var connection = new ClientConnection(socket);
        clientConnections.add(connection);
        logger.info("Client {} connected to server", connection.clientId);
        var clientConnectionThread = clientConnectionThreadFactory.newThread(() -> {
          try {
            connection.run();
          } catch (Exception e) {
            logger.error("Closing connection " + connection.clientId + " due to exception", e);
          } finally {
            connection.close();
          }
        });
        clientConnectionThread.setName("client-" + connection.seq + "-in");
        clientConnectionThread.start();
      } catch (SocketException e) {
        logger.info("Server socket closed");
        return;
      } catch (IOException e) {
        throw new IOExceptionWrapper(e);
      }
    }
  }

  private class ClientConnection extends SocketConnection {
    private String clientId;
    private final int seq = clientConnectionCounter.incrementAndGet();
    private final SocketAddress remoteAddress;
    private final ExecutorService writeExecutor = singleThreadExecutor("client-" + seq + "-out");
    private volatile Exception writeException = null;

    private ClientConnection(Socket socket) throws IOException {
      super(socket);
      remoteAddress = socket.getRemoteSocketAddress();
      this.clientId = remoteAddress.toString();
    }

    private void run() throws Exception {
      switch (readMark()) {
        case CLOSED -> {
          logger.info("Client {} closed connection", clientId);
          return;
        }
        case HANDSHAKE -> {
          clientId = readMessage(false).topic() + '/' + remoteAddress;
          logger.info("Received handshake request from {}", clientId);
          writeExecutor.execute(() -> {
            try {
              writeMessage(HANDSHAKE, seq, "", null);
              logger.debug("Sent handshake response to {}, seq={}", clientId, seq);
            } catch (Exception e) {
              logger.error("Could not send handshake response", e);
              breakWithException(e);
            }
          });
        }
        default -> throw new IllegalStateException("Unexpected mark byte. Client must send handshake as first request");
      }
      while (true) {
        if (writeException != null) {
          throw writeException;  // break the reading loop and propagate
        }
        var mark = readMark();
        switch (mark) {
          case CLOSED -> {
            logger.info("Client {} closed connection", clientId);
            return;
          }
          case SUBSCRIBE_REQUEST -> {
            var msg = readMessage(false);
            logger.info("Received subscription request id={} for topic={} from {}", msg.getId(), msg.topic(), clientId);
            topicSubscriptions
              .computeIfAbsent(msg.topic(), it -> new ConcurrentHashMap<>())
              .putIfAbsent(this, true);
            writeExecutor.execute(() -> {
              try {
                writeMessage(SUBSCRIBE_ACK, msg.id(), msg.topic(), null);
                logger.debug("Sent subscription ack id={} for topic={} to {}", msg.getId(), msg.topic(), clientId);
              } catch (Exception e) {
                logger.error("Could not send subscription ack id=" + msg.getId(), e);
                breakWithException(e);
              }
            });
          }
          case MESSAGE, SYNC_MESSAGE -> {
            var msg = readMessage(true);
            var sync = (mark == SYNC_MESSAGE);
            logger.debug("Received {} id={} for topic={} from {} ({} bytes)",
              (sync ? "sync message" : "message"), msg.getId(), msg.topic(), clientId, msg.getBody().length
            );
            var clients = topicSubscriptions.getOrDefault(msg.topic(), Collections.emptyMap()).keySet();
            if (clients.isEmpty()) {
              logger.debug("No clients subscribed topic={}, message dropped", msg.topic());
              if (sync) {
                writeExecutor.execute(() -> {
                  try {
                    writeMessage(MESSAGE_ACK, msg.id(), msg.topic(), null);
                    logger.debug("Sent immediate message ack id={} to {}", msg.getId(), clientId);
                  } catch (Exception e) {
                    logger.error("Could not sent message ack id=" + msg.getId(), e);
                    breakWithException(e);
                  }
                });
              }
            } else {
              if (sync) {
                pendingAcks.put(msg.id(), new PendingAck(this, new ConcurrentLinkedQueue<>(clients)));
              }
              logger.debug("Sending message to subscribed clients ({})", clients.size());
              for (var client : clients) {
                client.writeExecutor.execute(() -> {
                  try {
                    client.writeMessage((sync ? SYNC_MESSAGE : MESSAGE), msg.id(), msg.topic(), msg.body());
                    logger.debug("Sent {} id={} to {}", (sync ? "sync message" : "message"), msg.getId(), client.clientId);
                  } catch (Exception e) {
                    logger.error("Could not send message id=" + msg.getId(), e);
                    client.breakWithException(e);
                  }
                });
              }
            }
          }
          case MESSAGE_ACK -> {
            var msg = readMessage(false);
            logger.debug("Received message ack id={} from {}", msg.getId(), clientId);
            var pendingAck = pendingAcks.get(msg.id());
            if (pendingAck != null) {
              pendingAck.pendingClients.removeIf(it -> it == this);
              if (pendingAck.pendingClients.isEmpty() && pendingAcks.remove(msg.id()) != null) {
                var initiator = pendingAck.initiator;
                initiator.writeExecutor.execute(() -> {
                  try {
                    initiator.writeMessage(MESSAGE_ACK, msg.id(), msg.topic(), null);
                    logger.debug("Sent message ack id={} to {}", msg.getId(), initiator.clientId);
                  } catch (Exception e) {
                    logger.error("Could not sent message ack id=" + msg.getId(), e);
                    initiator.breakWithException(e);
                  }
                });
              }
            }
          }
          default -> throw new IllegalStateException("Unexpected mark 0x" + Integer.toHexString(mark));
        }
      }
    }

    private void breakWithException(Exception e) {
      writeException = e;
      try {
        socket.shutdownInput();  // break blocking read
      } catch (Exception ignored) {}
    }

    @Override
    void close() {
      super.close();
      clientConnections.remove(this);
      topicSubscriptions.values().forEach(it -> it.remove(this));
      logger.info("Connection closed for client {}", clientId);
    }
  }

  private record PendingAck(ClientConnection initiator, Queue<ClientConnection> pendingClients) {}
}