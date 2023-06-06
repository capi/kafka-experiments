package cc.dont_panic.experiments.kafka;

import cc.dont_panic.experiments.kafka.data.ChangeRequest;
import cc.dont_panic.experiments.kafka.data.ChangeRequestStreamGenerator;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.atomic.AtomicInteger;

public class ChangeRequestPublisher {
    private final KafkaConfig kafkaConfig;
    private final Producer<Long, ChangeRequest> producer;
    private final ChangeRequestStreamGenerator changeRequestStreamGenerator;

    private final boolean verbose;

    public ChangeRequestPublisher(KafkaConfig kafkaConfig, Producer<Long, ChangeRequest> producer, ChangeRequestStreamGenerator changeRequestStreamGenerator, boolean verbose) {
        this.kafkaConfig = kafkaConfig;
        this.producer = producer;
        this.changeRequestStreamGenerator = changeRequestStreamGenerator;
        this.verbose = verbose;
    }

    private void onCompletion(ChangeRequest changeRequest, RecordMetadata recordMetadata, Exception exception) {
        if (verbose) {
            System.out.println(changeRequest.getId() + ": Record written to partition " + recordMetadata.partition() + " offset " + recordMetadata.offset() + " timestamp " + recordMetadata.timestamp() + ": " + changeRequest);
        }
    }

    public void publishChanges(int numberOfChanges) {
        var changeStream = changeRequestStreamGenerator.createStream();
        AtomicInteger counter = new AtomicInteger();
        changeStream
                .takeWhile(cr -> counter.getAndIncrement() < numberOfChanges)
                .forEach(cr -> producer.send(producerRecordFor(cr), (m, e) -> onCompletion(cr, m, e)));
        System.out.println("Published " + (counter.get() - 1) + " records.");
    }

    private ProducerRecord<Long, ChangeRequest> producerRecordFor(ChangeRequest cr) {
        return new ProducerRecord<>(kafkaConfig.getChangeRequestsTopicName(), cr.getId(), cr);
    }
}
