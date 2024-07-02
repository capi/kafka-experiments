package cc.dont_panic.experiments.kafka;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TopicExistsException;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class KafkaTopicCreator {

    private final KafkaConfig kafkaConfig;

    public KafkaTopicCreator(KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
    }

    public void createMissingTopics() throws ExecutionException, InterruptedException {
        // https://www.baeldung.com/kafka-topic-creation

        Properties properties = new Properties();
        properties.put(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "tcp://localhost:9092"
        );

        try(Admin admin = Admin.create(properties)) {
            int partitions = 3;
            short replicationFactor = 1;

            // Create state-topic as a compacted topic with 'lz4' compression codec
            Map<String, String> stateTopicConfig = new HashMap<>();
            stateTopicConfig.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
            stateTopicConfig.put(TopicConfig.COMPRESSION_TYPE_CONFIG, "lz4");
            NewTopic stateTopic = new NewTopic(kafkaConfig.getStateTopicName(), partitions, replicationFactor)
                    .configs(stateTopicConfig);

            // Create change-requests-topic as a default topic with 'lz4 compression codec
            Map<String, String> changeRequestsTopicConfig = new HashMap<>();
            changeRequestsTopicConfig.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE);
            changeRequestsTopicConfig.put(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(Duration.ofDays(1).toMillis()));
            changeRequestsTopicConfig.put(TopicConfig.RETENTION_BYTES_CONFIG, String.valueOf(128L * 1024 * 1024));
            NewTopic resultTopic = new NewTopic(kafkaConfig.getChangeRequestsTopicName(), partitions, replicationFactor)
                    .configs(changeRequestsTopicConfig);

            CreateTopicsResult result = admin.createTopics(
                    List.of(stateTopic, resultTopic)
            );

            waitForCreated(result, kafkaConfig.getStateTopicName());
            waitForCreated(result, kafkaConfig.getChangeRequestsTopicName());
        }
    }

    private void waitForCreated(CreateTopicsResult result, String topicName) throws ExecutionException, InterruptedException {
        KafkaFuture<Void> future = result.values().get(topicName);
        try {
            future.get();
            System.out.println(topicName + " created.");
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TopicExistsException) {
                System.out.println( topicName + " already exists, continue.");
            } else {
                throw e;
            }
        }
    }
}
