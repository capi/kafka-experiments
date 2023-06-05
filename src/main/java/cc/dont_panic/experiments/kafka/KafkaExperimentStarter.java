package cc.dont_panic.experiments.kafka;

import cc.dont_panic.experiments.kafka.data.ChangeRequest;
import cc.dont_panic.experiments.kafka.data.ChangeRequestStreamGenerator;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.LongSerializer;

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

        System.out.println("Done");
    }

    private static Producer<Long, ChangeRequest> createChangeRequestProducer(KafkaConfig config) {
        Properties properties = new Properties();
        properties.put(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServer()
        );
        return new KafkaProducer<>(properties, new LongSerializer(), ChangeRequest.VALUE_SERIALIZER);
    }
}