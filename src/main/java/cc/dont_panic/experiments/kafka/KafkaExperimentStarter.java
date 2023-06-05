package cc.dont_panic.experiments.kafka;

import cc.dont_panic.experiments.kafka.data.ChangeRequest;
import cc.dont_panic.experiments.kafka.data.ChangeRequestStreamGenerator;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class KafkaExperimentStarter {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        KafkaConfig config = new KafkaConfig();
        KafkaTopicCreator kafkaTopicCreator = new KafkaTopicCreator(config);
        kafkaTopicCreator.createMissingTopics();

        try (Producer<Long, ChangeRequest> changeRequestProducer = createChangeRequestProducer(config)) {
            ChangeRequestStreamGenerator changeStreamGenerator = new ChangeRequestStreamGenerator(new Random(), 100, 20);
            ChangeRequestPublisher changeRequestPublisher = new ChangeRequestPublisher(config, changeRequestProducer, changeStreamGenerator);
            changeRequestPublisher.publishChanges(500);
        }

        try (Consumer<Long, ChangeRequest> changeRecordConsumer = createChangeRequestConsumer(config)) {
            ChangeRequestProcessor changeRequestProcessor = new ChangeRequestProcessor(config, changeRecordConsumer);
            changeRequestProcessor.consumeUntilIdle();
        }

        System.out.println("Done");
    }

    private static Producer<Long, ChangeRequest> createChangeRequestProducer(KafkaConfig config) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServer());
        return new KafkaProducer<>(properties, new LongSerializer(), ChangeRequest.VALUE_SERIALIZER);
    }

    private static Consumer<Long, ChangeRequest> createChangeRequestConsumer(KafkaConfig config) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServer());
        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "300000");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "change-request-processors");
        return new KafkaConsumer<>(properties, new LongDeserializer(), ChangeRequest.VALUE_DESERIALIZER);
    }
}