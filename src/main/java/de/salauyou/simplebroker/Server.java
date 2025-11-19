package de.salauyou.simplebroker;

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
import java.util.logging.Logger;

import static de.salauyou.simplebroker.SocketConnection.IOExceptionWrapper;
import static de.salauyou.simplebroker.SocketConnection.singleThreadExecutor;
import static java.util.logging.Level.*;

public class Server {

  private static final Logger logger = Logger.getLogger("server");

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
      logger.log(FINE, "Already started");
    }
    logger.log(INFO, "Starting the server at port {0}", port);
    try {
      serverSocket = new ServerSocket(port);
      clientConnectionListeningExecutor.execute(() -> {
        try {
          listenClientConnections();
        } catch (Exception e) {
          logger.log(SEVERE, "Stopping server due to exception", e);
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
      logger.log(FINE, "Not started");
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
        logger.log(INFO, "Client {0} connected to server", connection.clientId);
        var clientConnectionThread = clientConnectionThreadFactory.newThread(() -> {
          try {
            connection.run();
          } catch (Exception e) {
            logger.log(SEVERE, "Closing connection " + connection.clientId + " due to exception", e);
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
          logger.log(INFO, "Client {0} closed connection", clientId);
          return;
        }
        case HANDSHAKE -> {
          clientId = readMessage(false).topic() + '/' + remoteAddress;
          logger.log(INFO, "Received handshake request from {0}", clientId);
          writeExecutor.execute(() -> {
            try {
              writeMessage(HANDSHAKE, seq, "", null);
              logger.log(INFO, "Sent handshake response to {0}, seq={1}", new Object[]{clientId, seq});
            } catch (Exception e) {
              logger.log(SEVERE, "Could not send handshake response", e);
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
            logger.log(INFO, "Client {0} closed connection", clientId);
            return;
          }
          case SUBSCRIBE_REQUEST -> {
            var msg = readMessage(false);
            logger.log(INFO, "Received subscription request id={0} for topic={1} from {2}", new Object[]{msg.hexId(), msg.topic(), clientId});
            topicSubscriptions
              .computeIfAbsent(msg.topic(), it -> new ConcurrentHashMap<>())
              .putIfAbsent(this, true);
            writeExecutor.execute(() -> {
              try {
                writeMessage(SUBSCRIBE_ACK, msg.id(), msg.topic(), null);
                logger.log(FINE, "Sent subscription ack id={0} for topic={1} to {2}", new Object[]{msg.hexId(), msg.topic(), clientId});
              } catch (Exception e) {
                logger.log(SEVERE, "Could not send subscription ack id=" + msg.hexId(), e);
                breakWithException(e);
              }
            });
          }
          case MESSAGE, SYNC_MESSAGE -> {
            var msg = readMessage(true);
            var sync = (mark == SYNC_MESSAGE);
            logger.log(FINE, "Received {0} id={1} for topic={2} from {3} ({4} bytes)",
              new Object[]{(sync ? "sync message" : "message"), msg.hexId(), msg.topic(), clientId, msg.getBody().length}
            );
            var clients = topicSubscriptions.getOrDefault(msg.topic(), Collections.emptyMap()).keySet();
            if (clients.isEmpty()) {
              logger.log(FINE, "No clients subscribed topic={0}, message dropped", msg.topic());
              if (sync) {
                writeExecutor.execute(() -> {
                  try {
                    writeMessage(MESSAGE_ACK, msg.id(), msg.topic(), null);
                    logger.log(FINE, "Sent immediate message ack id={0} to {1}", new Object[]{msg.hexId(), clientId});
                  } catch (Exception e) {
                    logger.log(SEVERE, "Could not sent message ack id=" + msg.hexId(), e);
                    breakWithException(e);
                  }
                });
              }
            } else {
              if (sync) {
                pendingAcks.put(msg.id(), new PendingAck(this, new ConcurrentLinkedQueue<>(clients)));
              }
              logger.log(FINE, "Sending message to subscribed clients ({0})", clients.size());
              for (var client : clients) {
                client.writeExecutor.execute(() -> {
                  try {
                    client.writeMessage((sync ? SYNC_MESSAGE : MESSAGE), msg.id(), msg.topic(), msg.body());
                    logger.log(FINE, "Sent {0} id={1} to {2}", new Object[]{(sync ? "sync message" : "message"), msg.hexId(), client.clientId});
                  } catch (Exception e) {
                    logger.log(SEVERE, "Could not send message id=" + msg.hexId(), e);
                    client.breakWithException(e);
                  }
                });
              }
            }
          }
          case MESSAGE_ACK -> {
            var msg = readMessage(false);
            logger.log(FINE, "Received message ack id={0} from {1}", new Object[]{msg.hexId(), clientId});
            var pendingAck = pendingAcks.get(msg.id());
            if (pendingAck != null) {
              pendingAck.pendingClients.removeIf(it -> it == this);
              if (pendingAck.pendingClients.isEmpty() && pendingAcks.remove(msg.id()) != null) {
                var initiator = pendingAck.initiator;
                initiator.writeExecutor.execute(() -> {
                  try {
                    initiator.writeMessage(MESSAGE_ACK, msg.id(), msg.topic(), null);
                    logger.log(FINE, "Sent message ack id={0} to {1}", new Object[]{msg.hexId(), initiator.clientId});
                  } catch (Exception e) {
                    logger.log(SEVERE, "Could not sent message ack id=" + msg.hexId(), e);
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
      logger.log(INFO, "Connection closed for client {0}", clientId);
    }
  }

  private record PendingAck(ClientConnection initiator, Queue<ClientConnection> pendingClients) {}
}