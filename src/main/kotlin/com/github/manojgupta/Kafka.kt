package com.github.manojgupta

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import java.time.Duration
import java.util.*

//fun main(args: Array<String>) {
//    println("Hello, Kafka")
//}


data class Kafka(val bootstrapServers: String)

// Producer class
class Producer(kafka: Kafka, private val topic: String) {
    private val kafkaProducer: KafkaProducer<String, String>

    init {
        // create producer properties
        val config = Properties()
        config[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafka.bootstrapServers
        config[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        config[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java

        // create the producer
        kafkaProducer = KafkaProducer(config)
    }

    // send data
    fun send(msg: String) {
        val record = ProducerRecord<String, String>(topic, msg)
        kafkaProducer.send(record)
    }

    // flush data
    fun flush() = kafkaProducer.flush()

    // close producer
    fun close() = kafkaProducer.close()
}

// consumer class
class Consumer(kafka: Kafka, topic: String) {
    private val kafkaConsumer: KafkaConsumer<String, String>

    @Volatile
    var keepGoing = true

    init {
        // create consumer properties
        val config = Properties()
        config[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafka.bootstrapServers
        config[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        config[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        config[ConsumerConfig.GROUP_ID_CONFIG] = UUID.randomUUID().toString()
        config[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"

        // create the consumer
        kafkaConsumer = KafkaConsumer<String, String>(config)

        // Subscribe consumer to topic(s)
        kafkaConsumer.subscribe(listOf(topic))
    }

    fun consume(handler: (value: String) -> Unit) = Thread(Runnable {
        keepGoing = true
        kafkaConsumer.use { kc ->
            while (keepGoing) {
                kc.poll(Duration.ofMillis(500))?.forEach {
                    handler(it?.value() ?: "???")
                }
            }
        }
    }).start()

    fun stop() {
        keepGoing = false
    }
}