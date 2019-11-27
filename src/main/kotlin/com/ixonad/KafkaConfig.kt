package com.ixonad

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.streams.StreamsConfig
import java.util.*

object KafkaConfig {
    val p: Properties
        get() {
            val p = Properties()
            p.load(javaClass.getResourceAsStream("/credentials.conf"))
            return p
        }

    val BOOTSTRAP_SERVERS get() = p[AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG]
    val SCHEMA_REGISTRY_URL get() = p[AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG]
    val SCHEMA_REGISTRY_USER_INFO_CONFIG get() = p[AbstractKafkaAvroSerDeConfig.USER_INFO_CONFIG]

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
            put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "https")
            put(SaslConfigs.SASL_MECHANISM, "PLAIN")
            put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, "20000")
            put(SaslConfigs.SASL_JAAS_CONFIG, p.get(SaslConfigs.SASL_JAAS_CONFIG))
            put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL")
        }
    }
}