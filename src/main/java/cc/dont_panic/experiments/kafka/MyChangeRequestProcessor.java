package cc.dont_panic.experiments.kafka;

import cc.dont_panic.experiments.kafka.data.ChangeRequest;
import cc.dont_panic.experiments.kafka.data.PersistedProperty;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

public class MyChangeRequestProcessor implements FixedKeyProcessor<Long, ChangeRequest, ChangeRequest> {

    private FixedKeyProcessorContext<Long, ChangeRequest> context;
    private TimestampedKeyValueStore<String, PersistedProperty> stateStore;

    @Override
    public void init(FixedKeyProcessorContext<Long, ChangeRequest> context) {
        System.out.println("MyChangeRequestProcessor init()");
        this.context = context;
        this.stateStore = context.getStateStore("state-store");
        FixedKeyProcessor.super.init(context);
    }

    @Override
    public void process(FixedKeyRecord<Long, ChangeRequest> record) {
        var v = record.value();
        String key = PersistedProperty.keyFor(v.getId(), v.getPropertyName());
        ValueAndTimestamp<PersistedProperty> persistedProperty = stateStore.get(key);
        System.out.println("Approx size of stateStore=" + stateStore.approximateNumEntries());
        if (persistedProperty == null) {
            System.out.println("First time encountering " + key);
        } else {
            System.out.println("Currently contains: " + persistedProperty);
        }
        PersistedProperty value = new PersistedProperty(v.getId(), v.getPropertyName(), v.getPropertyValue());
        stateStore.put(key, ValueAndTimestamp.make(value, record.timestamp()));
        context.forward(record);
    }
}
