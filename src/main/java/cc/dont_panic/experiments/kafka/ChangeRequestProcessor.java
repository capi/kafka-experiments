package cc.dont_panic.experiments.kafka;

import cc.dont_panic.experiments.kafka.data.ChangeRequest;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;

public class ChangeRequestProcessor {
    private final KafkaConfig kafkaConfig;
    private final Consumer<Long, ChangeRequest> changeRecordConsumer;

    public ChangeRequestProcessor(KafkaConfig kafkaConfig, Consumer<Long, ChangeRequest> changeRecordConsumer) {
        this.kafkaConfig = kafkaConfig;
        this.changeRecordConsumer = changeRecordConsumer;
    }

    public void consumeUntilIdle() {
        changeRecordConsumer.subscribe(Collections.singleton(kafkaConfig.getChangeRequestsTopicName()));
        try {
            AtomicLong counter = new AtomicLong();
            long timeout = 10*1000; // wait up to 10 seconds for first batch, which can take some time due to partition assignment, then 0.5s
            while (true) {
                ConsumerRecords<Long, ChangeRequest> consumerRecords = changeRecordConsumer.poll(Duration.ofMillis(timeout));
                timeout = 500;
                if (!consumerRecords.isEmpty()) {
                    System.out.println("=== NEXT BATCH ===");
                    consumerRecords.forEach(record -> {
                        counter.incrementAndGet();
                        System.out.println("From partition " + record.partition() + " offset " + record.offset() + " consumed " + record.key() + ": " + record.value());
                    });
                } else {
                    changeRecordConsumer.commitSync(); // wait until we have committed
                    break;
                }
            }
            System.out.println("Processed " + counter.get() + " records.");

        } finally {
            changeRecordConsumer.unsubscribe();
        }

    }
}
