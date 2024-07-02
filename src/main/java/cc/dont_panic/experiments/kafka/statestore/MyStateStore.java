package cc.dont_panic.experiments.kafka.statestore;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.RecordBatchingStateRestoreCallback;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Base64;
import java.util.Collection;
import java.util.List;

public class MyStateStore implements KeyValueStore<Bytes, byte[]> {
    private final String stateStoreName;
    private StateStoreContext context;
    private File dataDir;
    private int partition = -1;
    private Position position;

    public MyStateStore(String stateStoreName) {
        this.stateStoreName = stateStoreName;
    }

    @Override
    public void put(Bytes key, byte[] value) {
        String fileName = getFileForKey(key);
        trace("Called put(" + fileName + ", " + value.length + ")");
        writeValue(fileName, value);
    }

    @Override
    public byte[] putIfAbsent(Bytes key, byte[] value) {
        String fileName = getFileForKey(key);
        trace("Called putIfAbsent(" + fileName + ", " + value.length + ")");
        byte[] old = readValue(fileName);
        if (old != null) {
            return old;
        } else {
            writeValue(fileName, value);
        }
        return null;
    }

    @Override
    public void putAll(List<KeyValue<Bytes, byte[]>> entries) {
        trace("Called putAll(" + entries.size() + ")");
        for (KeyValue<Bytes, byte[]> entry : entries) {
            put(entry.key, entry.value);
        }
    }

    @Override
    public byte[] delete(Bytes key) {
        String fileName = getFileForKey(key);
        trace("Called delete(" + fileName + ")");
        byte[] value = readValue(fileName);
        delete(fileName);
        return value;
    }

    @Override
    public String name() {
        return stateStoreName;
    }

    @Override
    @Deprecated
    public void init(ProcessorContext context, StateStore root) {
        StateStoreContext stateStoreContext = (StateStoreContext) context;
        init(stateStoreContext, root);
    }

    @Override
    public void init(StateStoreContext context, StateStore root) {
        this.context = context;
        this.partition = context.taskId().partition();
        this.dataDir = new File(context.stateDir(), name());
        trace("init(), partition=" + this.partition + "dataDir=" + this.dataDir + ", root=" + root);
        if (!this.dataDir.isDirectory()) {
            if (!this.dataDir.mkdirs()) {
                throw new ProcessorStateException("Failed creating " + this.dataDir);
            }
            throw new StreamsException("Data dir corrupt", context.taskId());
        }
        if (root != null) {
            context.register(root, (RecordBatchingStateRestoreCallback) this::restore, this::commit);
        }
    }

    private void restore(Collection<ConsumerRecord<byte[], byte[]>> records) {
        trace("Restoring " + records.size());
        for (ConsumerRecord<byte[], byte[]> record : records) {
            String fileName = getFileForKey(record.key());
            writeValue(fileName, record.value());
        }
    }

    private void commit() {
        trace("commit()");
    }

    @Override
    public void flush() {
        trace("Called flush()");
    }

    @Override
    public void close() {
        trace("Called flush");
    }

    @Override
    public boolean persistent() {
        return true;
    }

    @Override
    public boolean isOpen() {
        //trace("Called isOpen()");
        return true;
    }

    @Override
    public byte[] get(Bytes key) {
        String fileName = getFileForKey(key);
        trace("Called get(" + fileName +")");
        return readValue(fileName);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> range(Bytes from, Bytes to) {
        trace("Called range()");
        return new KeyValueIterator<Bytes, byte[]>() {
            @Override
            public void close() {
            }

            @Override
            public Bytes peekNextKey() {
                return null;
            }

            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public KeyValue<Bytes, byte[]> next() {
                return null;
            }
        };
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> all() {
        trace("Called all()");
        return new KeyValueIterator<Bytes, byte[]>() {
            @Override
            public void close() {
            }

            @Override
            public Bytes peekNextKey() {
                return null;
            }

            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public KeyValue<Bytes, byte[]> next() {
                return null;
            }
        };
    }

    @Override
    public long approximateNumEntries() {
        trace("Called approximateNumEntries()");
        try (var list = Files.list(dataDir.toPath())) {
            return list
                    .filter(p -> !p.getFileName().toString().startsWith("."))
                    .count();
        } catch (IOException e) {
            throw new ProcessorStateException("Failed to approximateNumEntries", e);
        }
    }

    void trace(String msg) {
        System.out.println(partition + ": " + msg);
    }

    @Override
    public Position getPosition() {
        return position;
    }

    private void writeValue(String fileName, byte[] value) {
        try (var out = new FileOutputStream(new File(dataDir, fileName))) {
            out.write(value);
        } catch (IOException e) {
            throw new ProcessorStateException("Error writing value to " + fileName, e);
        }
    }

    private byte[] readValue(String fileName) {
        var file = new File(dataDir, fileName);
        if (!file.exists()) return null;
        try {
            return Files.readAllBytes(file.toPath());
        } catch (IOException e) {
            throw new ProcessorStateException("Error reading value from " + fileName, e);
        }
    }

    private void delete(String fileName) {
        var file = new File(dataDir, fileName);
        try {
            Files.deleteIfExists(file.toPath());
        } catch (IOException e) {
            throw new ProcessorStateException("Error reading value from " + fileName, e);
        }
    }

    private static String getFileForKey(Bytes key) {
        byte[] bytes = key.get();
        return getFileForKey(bytes);
    }

    private static String getFileForKey(byte[] bytes) {
        return Base64.getEncoder().encodeToString(bytes);
    }
}
