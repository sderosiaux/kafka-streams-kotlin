package com.ixonad

import com.ixonad.KafkaConfig.addConfluentCloudConfig
import com.ixonad.avro.GameCreated
import com.ixonad.avro.GameDeleted
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import java.time.Duration
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.*
import kotlin.random.Random

object OneTopicTwoSubjects {
    const val TOPIC = "one-topic"

    @JvmStatic
    fun main(args: Array<String>) {
        createTopicsIfNecessary()
        produceRandomGameEvents()
        consumeEvents()
    }

    private fun consumeEvents() {
        val c = KafkaConsumer<String, SpecificRecord>(Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAP_SERVERS)
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SpecificAvroDeserializer::class.java)
            put(ConsumerConfig.GROUP_ID_CONFIG, "OneTopicTwoSubjects")
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            put(AbstractKafkaAvroSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy::class.java)
            put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, KafkaConfig.SCHEMA_REGISTRY_URL)
            put(AbstractKafkaAvroSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO")
            put(AbstractKafkaAvroSerDeConfig.USER_INFO_CONFIG, KafkaConfig.SCHEMA_REGISTRY_USER_INFO_CONFIG)
            addConfluentCloudConfig()
        })
        c.subscribe(listOf(TOPIC))

        while (true) {
            val records = c.poll(Duration.ofMillis(1000))
            records.forEach { r: ConsumerRecord<String, SpecificRecord> ->
                val x: String = when (val v = r.value()) {
                    is GameCreated -> "created ${v.gameId} with ${v.getPlayers()} players"
                    is GameDeleted -> "deleted ${v.gameId} at ${LocalDateTime.ofEpochSecond(
                        v.getWhen() / 1000, 0, ZoneOffset.UTC
                    )}"
                    else -> "not handled"
                }
                println(x)
            }
        }
    }

    private fun produceRandomGameEvents() {
        Thread {
            val p = KafkaProducer<String, SpecificRecord>(Properties().apply {
                put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAP_SERVERS)
                put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
                put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SpecificAvroSerializer::class.java)
                put(AbstractKafkaAvroSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy::class.java)
                put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, KafkaConfig.SCHEMA_REGISTRY_URL)
                put(AbstractKafkaAvroSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO")
                put(AbstractKafkaAvroSerDeConfig.USER_INFO_CONFIG, KafkaConfig.SCHEMA_REGISTRY_USER_INFO_CONFIG)
                addConfluentCloudConfig()
            })

            while (true) {
                val gameId = Random.nextInt(200).toString()
                val created = GameCreated("game_${gameId}", Random.nextInt(20))
                val deleted = GameDeleted("game_${gameId}", System.currentTimeMillis())
                p.send(ProducerRecord(TOPIC, gameId, created))
                p.send(ProducerRecord(TOPIC, gameId, deleted))
                Thread.sleep(1000)
            }
        }.start()
    }

    private fun createTopicsIfNecessary() {
        val admin = AdminClient.create(Properties().apply {
            put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAP_SERVERS)
            addConfluentCloudConfig()
        })
        val topics = admin.listTopics().names().get()
        val ourTopics = listOf(TOPIC)
        val needCreation = (ourTopics - topics)
        if (needCreation.isNotEmpty()) {
            admin.createTopics(needCreation.map { NewTopic(it, 3, 3) }).all().get()
        }
    }

}