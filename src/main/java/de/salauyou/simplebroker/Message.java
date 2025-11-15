package de.salauyou.simplebroker;

public interface Message {
    String getTopic();
    byte[] getBody();
}