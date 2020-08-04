package com.github.manojgupta

import org.slf4j.Logger
import org.slf4j.LoggerFactory


fun main(args: Array<String>) {
    val log: Logger = LoggerFactory.getLogger("com.github.manojgupta")

    val kafka = Kafka("localhost:9092")
    val topic = "kt-topic"

    val consumer = Consumer(kafka, topic)
    Runtime.getRuntime().addShutdownHook(Thread(Runnable {
        consumer.stop()
    }))
    consumer.consume {
        log.info("got $it")
    }
}