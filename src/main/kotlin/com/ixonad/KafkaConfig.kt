package com.ixonad

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import java.util.*

object KafkaConfig {
    const val BOOTSTRAP_SERVERS = "localhost:9092"
    const val TOPIC_INPUT = "prices"
    const val TOPIC_OUTPUT = "prices-updated"
    const val STORE = "toto"

    fun createTopicsIfNecessary() {
        val admin = AdminClient.create(Properties().apply {
            put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
        })
        val topics = admin.listTopics().names().get()
        val ourTopics = listOf(TOPIC_INPUT, TOPIC_OUTPUT)
        if (!topics.containsAll(ourTopics)) {
            admin.createTopics(ourTopics.map { NewTopic(it, 3, 1) }).all().get()
        }
    }
}