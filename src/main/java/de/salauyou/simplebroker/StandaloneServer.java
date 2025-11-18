package de.salauyou.simplebroker;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;

import static org.slf4j.Logger.ROOT_LOGGER_NAME;

public class StandaloneServer {

  public static void main(String[] args) {
    var port = args.length > 0 ? Integer.parseInt(args[0]) : 7000;
    if (args.length > 1) {
      ((Logger) LoggerFactory.getLogger(ROOT_LOGGER_NAME)).setLevel(Level.valueOf(args[1]));
    }
    var server = new Server(port);
    Runtime.getRuntime().addShutdownHook(new Thread(server::stop));
    server.start();
  }
}