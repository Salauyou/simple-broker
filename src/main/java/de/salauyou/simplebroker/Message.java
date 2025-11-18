package de.salauyou.simplebroker;

public interface Message {
  int getId();
  String getTopic();
  byte[] getBody();
}