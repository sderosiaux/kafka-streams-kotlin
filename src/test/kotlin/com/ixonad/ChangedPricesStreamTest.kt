package com.ixonad

import com.ixonad.json.JsonSerde.makeJsonSerde
import com.ixonad.ChangedPricesStream.buildTopology
import com.ixonad.KafkaConfig.TOPIC_INPUT
import com.ixonad.KafkaConfig.TOPIC_OUTPUT
import com.ixonad.model.GamePrice
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.test.ConsumerRecordFactory
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.*

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ChangedPricesStreamTest {

    private lateinit var factory: ConsumerRecordFactory<String, GamePrice>
    private lateinit var tdd: TopologyTestDriver

    private val serde = makeJsonSerde<GamePrice>()

    @BeforeAll
    fun init() {
        val props = Properties().apply {
            put(StreamsConfig.APPLICATION_ID_CONFIG, "changed-prices-test")
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "whocares:9092")
        }
        tdd = TopologyTestDriver(buildTopology(StreamsBuilder()), props)
        factory = ConsumerRecordFactory(StringSerializer(), serde.serializer())
    }

    @AfterAll
    fun teardown() = tdd.close()

    @Test
    fun `Send new prices only`() {
        // Given a gamePrice
        val gp = GamePrice("prd_123", 10.42)
        // When we send it
        sendGamePrice(gp)
        // Then we get the same record in output
        with(readOutput()!!) {
            assertKVEquals(gp)
        }

        // When we send the same gamePrice
        sendGamePrice(gp)
        // Then we don't get anything
        assertNull(readOutput())

        // When we send a different price twice
        val gpWithNewPrice = gp.copy(price = 1337.0)
        sendGamePrice(gpWithNewPrice)
        sendGamePrice(gpWithNewPrice)
        // Then we get the new price only once in output
        with(readOutput()!!) {
            assertKVEquals(gpWithNewPrice)
        }
        assertNull(readOutput())
    }

    private fun ProducerRecord<String, GamePrice>.assertKVEquals(gp: GamePrice) {
        assertEquals(key(), gp.gameId)
        assertEquals(value(), gp)
    }

    private fun sendGamePrice(gp: GamePrice) {
        tdd.pipeInput(factory.create(TOPIC_INPUT, gp.gameId, gp))
    }

    private fun readOutput(): ProducerRecord<String, GamePrice>? =
        tdd.readOutput(TOPIC_OUTPUT, StringDeserializer(), serde.deserializer())
}
