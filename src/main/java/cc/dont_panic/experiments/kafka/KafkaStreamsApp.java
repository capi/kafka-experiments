package cc.dont_panic.experiments.kafka;

import cc.dont_panic.experiments.kafka.data.ChangeRequest;
import cc.dont_panic.experiments.kafka.data.PersistedProperty;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class KafkaStreamsApp {

    private final KafkaConfig kafkaConfig;

    public KafkaStreamsApp(KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
    }

    public void run() {
        Properties streamsProps = new Properties();
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-streams-application");
        streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getBootstrapServer());
        streamsProps.put(StreamsConfig.STATE_DIR_CONFIG, ensureStateDir().toAbsolutePath().toString());
        streamsProps.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "2500");
        // https://docs.confluent.io/platform/current/streams/developer-guide/optimizing-streams.html
        // use source topic as change log topic
        streamsProps.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        // increase timeout to workaround timing issues with building the local state store
        // NOTE: may hang the processor!
        //streamsProps.put(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG, "3600000");

        StreamsBuilder builder = new StreamsBuilder();

        // with this timestamp extractor, we force Kafka to read the table before processing anything else
        // https://stackoverflow.com/questions/56556270/can-kafka-streams-be-configured-to-wait-for-ktable-to-load
        // NOTE: does not seem to work
        //TimestampExtractor tableTimestampExtractor = (r, pt) -> 1L;

        // TODO: If local state is lost, changes are streamed in before KTable is "current", resulting in wrong
        // "new" events, which could potentially cause corruption of the data in the topic

        // TODO: Custom partitioning strategy for state topic

        KTable<String, PersistedProperty> table = builder
                .table(kafkaConfig.getStateTopicName(),
                        Consumed.with(Serdes.String(), PersistedProperty.SERDE)
                                /*.withTimestampExtractor(tableTimestampExtractor)*/,
                        Materialized.<String, PersistedProperty, KeyValueStore<Bytes, byte[]>>as("state-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(PersistedProperty.SERDE));
        // flush the changes back to state topic
        table.toStream().to(kafkaConfig.getStateTopicName());

        KStream<Long, ChangeRequest> changeRequestStream = builder.stream(kafkaConfig.getChangeRequestsTopicName(),
                Consumed.with(Serdes.Long(), ChangeRequest.SERDE));
        changeRequestStream.processValues(MyChangeRequestProcessor::new, "state-store")
                .foreach((k, v) -> System.out.println("Received: " + k + ": " + v));


        Topology topology = builder.build(streamsProps);
        System.out.println(topology.describe().toString());
        try (KafkaStreams kafkaStreams = new KafkaStreams(topology, streamsProps)) {
            final CountDownLatch shutdownLatch = new CountDownLatch(1);
            addShutdownHook(kafkaStreams, shutdownLatch);
            try {
                kafkaStreams.setStateListener((newState, oldState) -> onStateChange(kafkaStreams, newState, oldState, shutdownLatch));
                kafkaStreams.setUncaughtExceptionHandler(t -> {
                    t.printStackTrace();
                    return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
                });
                kafkaStreams.start();

                shutdownLatch.await();
                System.out.println("Shutting down Kafka Streams...");
                kafkaStreams.close(Duration.ofSeconds(5));
                System.out.println("Kafka Streams shutdown complete");
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }
    }

    private static void onStateChange(KafkaStreams kafkaStreams, KafkaStreams.State newState, KafkaStreams.State oldState, CountDownLatch shutdownLatch) {
        System.out.println("State Change: " + oldState + "->" + newState);
        if (newState == KafkaStreams.State.RUNNING) {
            new Thread(() -> {
                try {
                    Thread.sleep(300_000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                shutdownLatch.countDown();
            }).start();

            ReadOnlyKeyValueStore<Long, ChangeRequest> store = kafkaStreams.store(StoreQueryParameters.fromNameAndType("state-store", QueryableStoreTypes.keyValueStore()));
            System.out.println("approx size=" + store.approximateNumEntries());
        }
    }

    private static void addShutdownHook(KafkaStreams kafkaStreams, CountDownLatch shutdownLatch) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            kafkaStreams.close(Duration.ofSeconds(2));
            shutdownLatch.countDown();
        }));
    }

    private Path ensureStateDir() {
        try {
            Path path = Path.of("kafka-streams-state-dir");
            Files.createDirectories(path);
            return path;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
