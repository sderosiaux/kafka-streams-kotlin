package com.ixonad.avro

import com.ixonad.KafkaConfig.BOOTSTRAP_SERVERS
import com.ixonad.KafkaConfig.SCHEMA_REGISTRY_URL
import com.ixonad.KafkaConfig.TOPIC_INPUT_AVRO
import com.ixonad.KafkaConfig.addConfluentCloudConfig
import com.ixonad.avro.GamePriceAvro
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.util.*
import kotlin.random.Random

// thread to produce random game price to make the streams work
fun produceRandomAvroGamePrice() {
    Thread {
        val p = KafkaProducer<String, GamePriceAvro>(Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SpecificAvroSerializer::class.java)
            put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL)
            addConfluentCloudConfig()
        })

        while (true) {
            val gp = GamePriceAvro("game_${Random.nextInt(200)}", Random.nextDouble(100.0))
            p.send(ProducerRecord(TOPIC_INPUT_AVRO, gp.gameId, gp)).get()
            Thread.sleep(1000)
        }
    }.start()
}
