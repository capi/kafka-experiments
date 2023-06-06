package cc.dont_panic.experiments.kafka;

import cc.dont_panic.experiments.kafka.data.ChangeRequest;
import cc.dont_panic.experiments.kafka.data.PersistedProperty;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.ArrayList;
import java.util.List;

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

        boolean needsUpdate = persistedProperty == null;

        List<String> newList = new ArrayList<>(3);
        if (persistedProperty != null) {
            newList.addAll(persistedProperty.value().getValues());
        }
        if (!newList.contains(v.getPropertyValue())) {
            if (newList.size() >= 3) newList.remove(0);
            newList.add(v.getPropertyValue());
            needsUpdate = true;
        }

        if (needsUpdate) {
            PersistedProperty value = new PersistedProperty(v.getId(), v.getPropertyName(), newList);
            stateStore.put(key, ValueAndTimestamp.make(value, record.timestamp()));
            System.out.println("New state: " + value);
        }

        context.forward(record);
    }
}
