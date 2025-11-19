package de.salauyou.simplebroker;

public class StandaloneServer {

  public static void main(String[] args) {
    var port = args.length > 0 ? Integer.parseInt(args[0]) : 7000;
    var server = new Server(port);
    Runtime.getRuntime().addShutdownHook(new Thread(server::stop));
    server.start();
  }
}