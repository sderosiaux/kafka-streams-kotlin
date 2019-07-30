package com.ixonad

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.streams.StreamsConfig
import java.util.*

object KafkaConfig {
    const val BOOTSTRAP_SERVERS = "localhost:9092" // XXX.europe-west1.gcp.confluent.cloud:9092"
    const val SCHEMA_REGISTRY_URL = "http://localhost:8081"
    const val TOPIC_INPUT = "prices"
    const val TOPIC_OUTPUT = "prices-updated"
    const val TOPIC_INPUT_AVRO = "prices-avro"
    const val TOPIC_OUTPUT_AVRO = "prices-updated-avro"
    const val STORE = "last-prices-store"

    fun createTopicsIfNecessary() {
        val admin = AdminClient.create(Properties().apply {
            put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
            addConfluentCloudConfig()
        })
        val topics = admin.listTopics().names().get()
        val ourTopics = listOf(TOPIC_INPUT, TOPIC_OUTPUT, TOPIC_INPUT_AVRO, TOPIC_OUTPUT_AVRO)
        val needCreation = (ourTopics - topics)
        if (needCreation.isNotEmpty()) {
            admin.createTopics(needCreation.map { NewTopic(it, 3, 1) }).all().get()
        }
    }

    fun Properties.addConfluentCloudConfig() {
        if (this[CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG].toString().contains("confluent.cloud")) {
            put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3)
            put("ssl.endpoint.identification.algorithm", "https")
            put("sasl.mechanism", "PLAIN")
            put("request.timeout.ms", "20000")
            put(
                "sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"<USERNAME>\" password=\"<PASSWORD>\";"
            )
            put("security.protocol", "SASL_SSL")
        }
    }
}