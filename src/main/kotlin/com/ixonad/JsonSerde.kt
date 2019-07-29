package com.ixonad

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer

object JsonSerde {
    inline fun <reified T> makeJsonSerde(): Serde<T> {
        return object : Serde<T> {
            private val mapper = jacksonObjectMapper()

            override fun deserializer(): Deserializer<T> =
                Deserializer { _, data -> mapper.readValue(data, T::class.java) }

            override fun serializer(): Serializer<T> =
                Serializer { _, data -> mapper.writeValueAsBytes(data) }
        }
    }

}