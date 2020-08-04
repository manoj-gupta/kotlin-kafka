package com.github.manojgupta

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.LocalDateTime
import java.util.*

fun main(args: Array<String>) {
    val log: Logger = LoggerFactory.getLogger("com.github.manojgupta")

    val kafka = Kafka("localhost:9092")
    val topic = "kt-topic"

    val producer = Producer(kafka, topic)
    (1..10).forEach {
        val msg = "new message $it ${LocalDateTime.now()}"
        log.info("sending $msg")
        producer.send(msg)
    }

    // flash data as send is asynchronous
    producer.flush()

    // close producer
    producer.close()
}