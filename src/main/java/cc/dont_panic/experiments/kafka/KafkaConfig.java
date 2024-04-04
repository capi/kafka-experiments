package cc.dont_panic.experiments.kafka;

import cc.dont_panic.experiments.kafka.infra.InstanceIdProvider;

public class KafkaConfig {

    private final String bootstrapServer = "tcp://localhost:9092";

    private final String stateTopicName = "state-topic";

    private final String changeRequestsTopicName = "change-requests-topic";

    private final InstanceIdProvider instanceIdProvider;

    public KafkaConfig(InstanceIdProvider instanceIdProvider) {
        this.instanceIdProvider = instanceIdProvider;
    }

    public String getBootstrapServer() {
        return bootstrapServer;
    }

    public String getStateTopicName() {
        return stateTopicName;
    }

    public String getChangeRequestsTopicName() {
        return changeRequestsTopicName;
    }

    public String getInstanceId() {
        return "instance-" + instanceIdProvider.getInstanceId() ;
    }
}
