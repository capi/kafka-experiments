package cc.dont_panic.experiments.kafka;

public class KafkaConfig {

    private final String bootstrapServer = "tcp://localhost:9092";

    private final String stateTopicName = "state-topic";

    private final String changeRequestsTopicName = "change-requests-topic";

    public String getBootstrapServer() {
        return bootstrapServer;
    }

    public String getStateTopicName() {
        return stateTopicName;
    }

    public String getChangeRequestsTopicName() {
        return changeRequestsTopicName;
    }
}
