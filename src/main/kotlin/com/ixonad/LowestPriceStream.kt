package com.ixonad

import com.ixonad.JsonSerde.makeJsonSerde
import org.apache.kafka.common.serialization.Serdes
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


typealias GameId = String
typealias Price = Double

data class GamePrice(val gameId: GameId, val price: Price)

object LowestPriceStream {
    const val TOPIC_INPUT = "prices"
    const val TOPIC_OUTPUT = "prices-updated"
    const val STORE = "toto"

    @JvmStatic
    fun main(args: Array<String>) {
        val props = Properties().apply {
            put(StreamsConfig.APPLICATION_ID_CONFIG, "lowest-price")
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        }
        val streams = KafkaStreams(buildTopology(StreamsBuilder()), props)

        streams.cleanUp()
        streams.start()

        Runtime.getRuntime().addShutdownHook(Thread(streams::close))
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