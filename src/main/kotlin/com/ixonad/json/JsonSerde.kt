package com.ixonad.json

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.ixonad.model.GamePrice
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

// Stupid crap for the Producer (it needs a reference because it does reflection)
class GamePriceSerializer: Serializer<GamePrice> {
    private val ser = JsonSerde.makeJsonSerde<GamePrice>().serializer()
    override fun serialize(topic: String?, data: GamePrice?): ByteArray = ser.serialize(topic, data)

}