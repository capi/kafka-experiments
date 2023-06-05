package cc.dont_panic.experiments.kafka;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.TopicConfig;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaExperimentStarter {

    private static final String STATE_TOPIC = "state-topic";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        KafkaConfig config = new KafkaConfig();
        KafkaTopicCreator kafkaTopicCreator = new KafkaTopicCreator(config);
        kafkaTopicCreator.createMissingTopics();

        System.out.println("Done");
    }
}