package cc.dont_panic.experiments.kafka.statestore;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

public class MyStateStores {
    private final String stateStoreName;

    public MyStateStores(String stateStoreName) {
        this.stateStoreName = stateStoreName;
    }

    public static MyStateStores builder(String stateStoreName) {
        return new MyStateStores(stateStoreName);
    }

    public KeyValueBytesStoreSupplier keyValueStore() {
        return new KeyValueBytesStoreSupplier() {
            @Override
            public String name() {
                return stateStoreName;
            }

            @Override
            public KeyValueStore<Bytes, byte[]> get() {
                return new MyStateStore(stateStoreName);
            }

            @Override
            public String metricsScope() {
                return "mystatestore";
            }
        };
    }
}
