package com.ixonad

import com.ixonad.KafkaConfig.BOOTSTRAP_SERVERS
import com.ixonad.KafkaConfig.TOPIC_INPUT
import com.ixonad.KafkaConfig.addConfluentCloudConfig
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.util.*
import kotlin.random.Random

object GamePriceProducer {

    // thread to produce random game price to make the streams work
    fun produceRandomGamePrice() {
        Thread {
            val p = KafkaProducer<String, GamePrice>(Properties().apply {
                put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
                put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
                put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GamePriceSerializer::class.java)
                addConfluentCloudConfig()
            })

            while (true) {
                val gp = GamePrice("game_${Random.nextInt(200)}", Random.nextDouble(100.0))
                p.send(ProducerRecord(TOPIC_INPUT, gp.gameId, gp)).get()
                Thread.sleep(1000)
            }
        }.start()
    }
}