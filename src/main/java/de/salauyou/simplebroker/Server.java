package de.salauyou.simplebroker;

import javafx.util.Pair;
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

  private static final Logger logger = LoggerFactory.getLogger(Server.class);

  private final int port;
  private final Executor clientConnectionListeningExecutor = singleThreadExecutor("server");
  private final ThreadFactory clientConnectionThreadFactory = Executors.defaultThreadFactory();
  private final AtomicInteger clientConnectionCounter = new AtomicInteger();
  private final Queue<ClientConnection> clientConnections = new ConcurrentLinkedQueue<>();
  private final Map<String, Map<ClientConnection, Boolean>> topicSubscriptions = new ConcurrentHashMap<>();
  private final Map<Long, Pair<ClientConnection, Queue<ClientConnection>>> pendingSyncRequests = new ConcurrentHashMap<>();

  private volatile ServerSocket serverSocket;

  public Server(int port) {
    this.port = port;
  }

  synchronized void start() {
    if (serverSocket != null) {
      logger.info("Already started");
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

  synchronized void stop() {
    if (serverSocket == null) {
      logger.info("Not started");
    }
    logger.info("Stopping the server");
    clientConnections.forEach(ClientConnection::close);
    try {
      serverSocket.close();
    } catch (IOException ignored) {}
    topicSubscriptions.clear();
    pendingSyncRequests.clear();
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
            if (e instanceof SocketConnection.IOExceptionWrapper wrapper) {
              e = wrapper.cause;
            }
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
    private final int seq = clientConnectionCounter.incrementAndGet();
    private final ExecutorService replyExecutor = singleThreadExecutor("client-" + seq + "-out");
    private final SocketAddress remoteAddress;
    private String clientId;

    private ClientConnection(Socket socket) throws IOException {
      super(socket);
      remoteAddress = socket.getRemoteSocketAddress();
      this.clientId = remoteAddress.toString();
    }

    void run() throws IOException {
      while (true) {
        if (isClosed()) {
          logger.info("Connection with {} is closed", clientId);
          return;
        }
        var mark = readMark();
        switch (mark) {
          case CLOSED -> {
            logger.info("Client {} closed connection", clientId);
            close();
            return;
          }
          case CLIENT_ID -> {
            var id = readText();
            clientId = id + remoteAddress;
            logger.info("Received clientId from {}", clientId);
          }
          case SUBSCRIBE_REQUEST -> {
            var request = readUtilityMessage();
            logger.info("Received subscription request for topic=${request.topic} from $clientId");
            topicSubscriptions.computeIfAbsent(request.topic(), it -> new ConcurrentHashMap<>()).putIfAbsent(this, true);
            replyExecutor.execute(() -> {
              try {
                writeUtilityMessage(SUBSCRIBE_ACK, request.topic(), 0);
                logger.info("Sent subscription ack for topic={} client={}", request.topic(), clientId);
              } catch (IOException e) {
                throw new IOExceptionWrapper(e);
              }
            });
          }
          case MESSAGE -> {
            var message = readMessage();
            var topic = message.getTopic();
            logger.info("Received message for topic={} from {} ({} bytes)", topic, clientId, message.getBody().length);
            var clients = topicSubscriptions.getOrDefault(topic, Collections.emptyMap());
            if (clients.isEmpty()) {
              logger.info("No clients subscribed topic={}, message dropped", topic);
            } else {
              logger.info("Sending message to subscribed clients ({})", clients.size());
              clients.keySet().forEach(it -> {
                it.replyExecutor.execute(() -> {
                  try {
                    it.writeMessage(message);
                    logger.info("Sent message to {}", it.clientId);
                  } catch (IOException e) {
                    throw new IOExceptionWrapper(e);
                  }
                });
              });
            }
          }
          case SYNC_REQUEST -> {
            var request = readUtilityMessage();
            logger.info("Received sync request for {} from {}", request, clientId);
            var subscribedClients = topicSubscriptions.getOrDefault(request.topic(), Collections.emptyMap());
            if (subscribedClients.isEmpty()) {
              replyExecutor.execute(() -> {
                try {
                  writeUtilityMessage(SYNC_ACK, request.topic(), request.id());
                  logger.info("No clients subscribed topic={}, sent sync ack to {}", request.topic(), clientId);
                } catch (IOException e) {
                  throw new IOExceptionWrapper(e);
                }
              });
            } else {
              subscribedClients.keySet().forEach(client -> {
                pendingSyncRequests.computeIfAbsent(request.id(), it -> new Pair<>(this, new ConcurrentLinkedQueue<>()))
                  .getValue()
                  .add(client);
                client.replyExecutor.execute(() -> {
                  try {
                    client.writeUtilityMessage(SYNC_REQUEST, request.topic(), request.id());
                    logger.info("Sent sync request for {} to {}", request, client.clientId);
                  } catch (IOException e) {
                    throw new IOExceptionWrapper(e);
                  }
                });
              });
            }
          }
          case SYNC_ACK -> {
            var response = readUtilityMessage();
            logger.info("Received sync ack for {} from {}", response, clientId);
            var pendingSyncRequest = pendingSyncRequests.get(response.id());
            if (pendingSyncRequest != null) {
              var initiator = pendingSyncRequest.getKey();
              var awaitingClients = pendingSyncRequest.getValue();
              awaitingClients.removeIf(it -> it == this);
              if (awaitingClients.isEmpty()) {
                initiator.replyExecutor.execute(() -> {
                  try {
                    initiator.writeUtilityMessage(SYNC_ACK, response.topic(), response.id());
                    logger.info("Sent sync ack for {} to {}", response, initiator.clientId);
                  } catch (IOException e) {
                    throw new IOExceptionWrapper(e);
                  }
                });
              }
            }
          }
          default -> logger.error("Unexpected mark byte {}", mark);
        }
      }
    }

    @Override
    void close() {
      clientConnections.remove(this);
      topicSubscriptions.values().forEach(it -> it.remove(this));
      replyExecutor.shutdown();
      super.close();
    }
  }
}