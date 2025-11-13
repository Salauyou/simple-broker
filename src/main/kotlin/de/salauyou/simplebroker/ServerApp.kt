package de.salauyou.simplebroker

fun main(args: Array<String>) {
    val port = args.takeUnless { it.isEmpty() }?.let { it[0].toInt() } ?: 6999
    Server(port).start()
    Thread.currentThread().join()
}