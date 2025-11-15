package de.salauyou.simplebroker;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

abstract class SocketConnection {

  // mark bytes
  protected final static byte CLOSED = -1;
  protected final static byte CLIENT_ID = 0;
  protected final static byte SUBSCRIBE_REQUEST = 1;
  protected final static byte SUBSCRIBE_ACK = 2;
  protected final static byte MESSAGE = 3;
  protected final static byte SYNC_REQUEST = 4;
  protected final static byte SYNC_ACK = 5;

  private final Socket socket;
  protected final InputStream input;
  protected final OutputStream output;

  SocketConnection(Socket socket) throws IOException {
    this.socket = socket;
    this.input = socket.getInputStream();
    this.output = socket.getOutputStream();
  }

  byte readMark() throws IOException {
    try {
      return (byte) input.read();
    } catch (SocketException se) {
      return CLOSED;
    }
  }

  void writeClientId(String id) throws IOException {
    synchronized (output) {
      var idBytes = id.getBytes(StandardCharsets.UTF_8);
      var bytes = ByteBuffer.allocate(5 + idBytes.length)
        .put(CLIENT_ID)           // mark - 1b
        .putInt(idBytes.length)   // id length - 4b
        .put(idBytes)             // id
        .array();
      output.write(bytes);
      output.flush();
    }
  }

  void writeUtilityMessage(byte type, String topic, long id) throws IOException {
    synchronized (output) {
      var topicNameBytes = topic.getBytes(StandardCharsets.UTF_8);
      var bytes = ByteBuffer.allocate(13 + topicNameBytes.length)
        .put(type)                       // mark - 1b
        .putInt(topicNameBytes.length)   // topic length - 4b
        .put(topicNameBytes)             // topic
        .putLong(id)                     // id - 8b
        .array();
      output.write(bytes);
      output.flush();
    }
  }

  void writeMessage(Message message) throws IOException {
    synchronized (output) {
      var topicBytes = message.getTopic().getBytes(StandardCharsets.UTF_8);
      var bytes = ByteBuffer.allocate(9 + topicBytes.length)
        .put(MESSAGE)                       // mark - 1b
        .putInt(topicBytes.length)          // topic length - 4b
        .put(topicBytes)                    // topic
        .putInt(message.getBody().length)   // message length - 4b
        .array();
      output.write(bytes);
      output.write(message.getBody());      // message
      output.flush();
    }
  }

  UtilityMessage readUtilityMessage() throws IOException {
    var topic = readText();
    var id = ByteBuffer.wrap(input.readNBytes(8)).getLong();
    return new UtilityMessage(topic, id);
  }

  Message readMessage() throws IOException {
    var topic = readText();
    var messageLength = ByteBuffer.wrap(input.readNBytes(4)).getInt();
    var bytes = input.readNBytes(messageLength);
    return new MessageImpl(topic, bytes);
  }

  String readText() throws IOException {
    var topicNameLength = ByteBuffer.wrap(input.readNBytes(4)).getInt();
    return new String(input.readNBytes(topicNameLength), StandardCharsets.UTF_8);
  }

  void close() {
    try {
      socket.shutdownInput();
    } catch (IOException ignored) {}
    try {
      socket.shutdownOutput();
    } catch (IOException ignored) {}
    try {
      socket.close();
    } catch (IOException ignored) {}
  }

  boolean isClosed() {
    return socket.isClosed();
  }

  static ExecutorService singleThreadExecutor(String threadName) {
    var executor = Executors.newSingleThreadExecutor();
    executor.submit(() -> {
      Thread.currentThread().setName(threadName);
    });
    return executor;
  }

  record MessageImpl(String topic, byte[] bytes) implements Message {

    @Override
    public String getTopic() {
      return topic;
    }

    @Override
    public byte[] getBody() {
      return bytes;
    }
  }

  record UtilityMessage(String topic, long id) {
    @Override
    public String toString() {
      return "topic=" + topic + ", id=" + Long.toHexString(id);
    }
  }

  static class IOExceptionWrapper extends RuntimeException {
    final IOException cause;
    IOExceptionWrapper(IOException e) {
      super(e);
      cause = e;
    }
  }
}