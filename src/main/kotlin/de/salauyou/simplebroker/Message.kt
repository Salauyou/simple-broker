package de.salauyou.simplebroker

interface Message {
    fun getTopic(): String
    fun getBody() : ByteArray
}