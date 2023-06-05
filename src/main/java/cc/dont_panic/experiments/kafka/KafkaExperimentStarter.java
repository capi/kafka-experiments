package cc.dont_panic.experiments.kafka;

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