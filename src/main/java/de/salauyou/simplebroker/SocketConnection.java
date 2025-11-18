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
  protected final static int CLOSED = -1;
  protected final static int HANDSHAKE = 1;
  protected final static int SUBSCRIBE_REQUEST = 2;
  protected final static int SUBSCRIBE_ACK = 3;
  protected final static int MESSAGE = 4;
  protected final static int SYNC_MESSAGE = 5;
  protected final static int MESSAGE_ACK = 6;

  protected final Socket socket;
  protected final InputStream input;
  protected final OutputStream output;

  private final ByteBuffer buffer = ByteBuffer.allocate(270);  // reusable write buffer

  SocketConnection(Socket socket) throws IOException {
    this.socket = socket;
    this.input = socket.getInputStream();
    this.output = socket.getOutputStream();
  }

  int readMark() throws IOException {
    try {
      return input.read();
    } catch (SocketException se) {
      return CLOSED;
    }
  }

  void writeMessage(int mark, int id, String topic, byte[] body) throws IOException {
    var topicBytes = topic.getBytes(StandardCharsets.UTF_8);
    checkLength(topicBytes);
    buffer.clear()
      .put((byte) mark)
      .putInt(id)
      .put((byte) topicBytes.length)
      .put(topicBytes);
    if (body != null) {
      buffer.putInt(body.length);
    }
    output.write(buffer.array(), 0, buffer.position());
    if (body != null) {
      output.write(body);
    }
    output.flush();
  }

  MessageImpl readMessage(boolean hasBody) throws IOException {
    var id = ByteBuffer.wrap(input.readNBytes(4)).getInt();
    var topicLength = input.read();
    var topic = new String(input.readNBytes(topicLength), StandardCharsets.UTF_8);
    byte[] body = null;
    if (hasBody) {
      var bodyLength = ByteBuffer.wrap(input.readNBytes(4)).getInt();
      body = input.readNBytes(bodyLength);
    }
    return new MessageImpl(id, topic, body);
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

  static void checkLength(byte[] bytes) {
    if (bytes.length > 255) {
      throw new IllegalArgumentException("Maximum allowed length 255 bytes");
    }
  }

  static ExecutorService singleThreadExecutor(String threadName) {
    var executor = Executors.newSingleThreadExecutor();
    executor.submit(() -> {
      Thread.currentThread().setName(threadName);
    });
    return executor;
  }

  record MessageImpl(int id, String topic, byte[] body) implements Message {
    @Override
    public int getId() {
      return id;
    }

    @Override
    public String getTopic() {
      return topic;
    }

    @Override
    public byte[] getBody() {
      return body;
    }

    String hexId() {
      return Integer.toHexString(id);
    }
  }

  static class IOExceptionWrapper extends RuntimeException {
    IOExceptionWrapper(IOException e) {
      super(e);
    }
  }
}