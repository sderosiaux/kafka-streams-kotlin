package com.ixonad

import com.ixonad.JsonSerde.makeJsonSerde
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.*
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.kstream.TransformerSupplier
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.processor.WallclockTimestampExtractor
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.Stores
import java.util.*
import kotlin.random.Random


typealias GameId = String
typealias Price = Double

data class GamePrice(val gameId: GameId, val price: Price)

object ChangedPricesStream {
    private const val BOOTSTRAP_SERVERS = "localhost:9092"

    const val TOPIC_INPUT = "prices"
    const val TOPIC_OUTPUT = "prices-updated"
    const val STORE = "toto"

    @JvmStatic
    fun main(args: Array<String>) {
        createTopicsIfNecessary()

        // thread to produce random game price to make the streams work
        Thread { produceRandomGamePrice() }.start()

        val props = Properties().apply {
            put(StreamsConfig.APPLICATION_ID_CONFIG, "changed-prices")
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
        }
        val streams = KafkaStreams(buildTopology(StreamsBuilder()), props)

        streams.cleanUp()
        streams.start()

        Runtime.getRuntime().addShutdownHook(Thread(streams::close))
    }

    private fun produceRandomGamePrice() {
        val p = KafkaProducer<String, GamePrice>(Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GamePriceSerializer::class.java)
        })

        while (true) {
            val gp = GamePrice("game_${Random.nextInt(200)}", Random.nextDouble(100.0))
            p.send(ProducerRecord(TOPIC_INPUT, gp.gameId, gp)).get()
            Thread.sleep(1000)
        }
    }

    private fun createTopicsIfNecessary() {
        val admin = AdminClient.create(Properties().apply {
            put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
        })
        val topics = admin.listTopics().names().get()
        val ourTopics = listOf(TOPIC_INPUT, TOPIC_OUTPUT)
        if (!topics.containsAll(ourTopics)) {
            admin.createTopics(ourTopics.map { NewTopic(it, 3, 1) }).all().get()
        }
    }

    // visible for testing
    fun buildTopology(sb: StreamsBuilder): Topology {
        val serdeSource = Consumed.with(
            Serdes.String(),
            makeJsonSerde<GamePrice>(),
            WallclockTimestampExtractor(),
            Topology.AutoOffsetReset.EARLIEST
        )
        val sinkSerde = Produced.with(Serdes.String(), makeJsonSerde<GamePrice>())


        // make a state to keep our price we're going to compare to
        sb.addStateStore(
            Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(STORE),
                Serdes.StringSerde(),
                Serdes.DoubleSerde()
            )
        )

        // the real deal: the topology
        sb.stream(TOPIC_INPUT, serdeSource)
            .transform(makeDedupTransformer(), STORE)
            .to(TOPIC_OUTPUT, sinkSerde)

        return sb.build()
    }

    private fun makeDedupTransformer(): TransformerSupplier<String, GamePrice, KeyValue<String, GamePrice>> =
        TransformerSupplier {
            object : Transformer<String, GamePrice, KeyValue<String, GamePrice>> {
                private lateinit var context: ProcessorContext
                private lateinit var store: KeyValueStore<String, Price>

                @Suppress("UNCHECKED_CAST")
                override fun init(context: ProcessorContext) {
                    this.context = context
                    this.store = context.getStateStore(STORE) as KeyValueStore<String, Price>
                }

                override fun transform(key: String, value: GamePrice): KeyValue<String, GamePrice>? {
                    val existingKey: Price? = store.get(key)
                    return when {
                        existingKey == null || existingKey != value.price -> {
                            store.put(key, value.price)
                            KeyValue.pair(key, value)
                        }
                        else -> null
                    }
                }

                override fun close() {}
            }
        }
}