package com.ixonad

import com.ixonad.KafkaConfig.BOOTSTRAP_SERVERS
import com.ixonad.KafkaConfig.SCHEMA_REGISTRY_URL
import com.ixonad.KafkaConfig.STORE
import com.ixonad.KafkaConfig.TOPIC_INPUT_AVRO
import com.ixonad.KafkaConfig.TOPIC_OUTPUT_AVRO
import com.ixonad.KafkaConfig.addConfluentCloudConfig
import com.ixonad.KafkaConfig.createTopicsIfNecessary
import com.ixonad.model.Price
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import com.ixonad.avro.GamePriceAvro
import com.ixonad.avro.produceRandomAvroGamePrice
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
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

object ChangedPricesStreamAvro {

    @JvmStatic
    fun main(args: Array<String>) {
        createTopicsIfNecessary()
        produceRandomAvroGamePrice()

        val streams = KafkaStreams(buildTopology(StreamsBuilder()), Properties().apply {
            put(StreamsConfig.APPLICATION_ID_CONFIG, "changed-prices")
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
            addConfluentCloudConfig()
        })

        streams.cleanUp()
        streams.start()

        Runtime.getRuntime().addShutdownHook(Thread(streams::close))
    }

    // visible for testing
    fun buildTopology(sb: StreamsBuilder): Topology {
        val gamePriceSerde = SpecificAvroSerde<GamePriceAvro>().apply {
            configure(mapOf(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG to SCHEMA_REGISTRY_URL), false)
        }

        val serdeSource = Consumed.with(
            Serdes.String(),
            gamePriceSerde,
            WallclockTimestampExtractor(),
            Topology.AutoOffsetReset.EARLIEST
        )
        val sinkSerde = Produced.with(Serdes.String(), gamePriceSerde)

        // make a state to keep our price we're going to compare to
        sb.addStateStore(
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(STORE),
                Serdes.StringSerde(),
                Serdes.DoubleSerde()
            )
        )

        // the real deal: the topology
        sb.stream(TOPIC_INPUT_AVRO, serdeSource)
            .transform(makeDedupTransformer(), STORE)
            .to(TOPIC_OUTPUT_AVRO, sinkSerde)

        return sb.build()
    }

    private fun makeDedupTransformer(): TransformerSupplier<String, GamePriceAvro, KeyValue<String, GamePriceAvro>> =
        TransformerSupplier {
            object : Transformer<String, GamePriceAvro, KeyValue<String, GamePriceAvro>> {
                private lateinit var context: ProcessorContext
                private lateinit var store: KeyValueStore<String, Price>

                @Suppress("UNCHECKED_CAST")
                override fun init(context: ProcessorContext) {
                    this.context = context
                    this.store = context.getStateStore(STORE) as KeyValueStore<String, Price>
                }

                override fun transform(key: String, value: GamePriceAvro): KeyValue<String, GamePriceAvro>? {
                    val existingKey: Price? = store.get(key)
                    return when {
                        existingKey == null || existingKey != value.getPrice() -> {
                            store.put(key, value.getPrice())
                            KeyValue.pair(key, value)
                        }
                        else -> null
                    }
                }

                override fun close() {}
            }
        }
}