package cc.dont_panic.experiments.kafka.data;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class PersistedProperty {

    public static final Serializer<PersistedProperty> VALUE_SERIALIZER = (topic, data) -> {
        StringBuilder valueStr = new StringBuilder();
        if (data.values != null) {
            for (String value : data.values) {
                if (valueStr.length() > 0) valueStr.append(",");
                valueStr.append(value);
            }
        }

        return (data.id + ":" + data.property + ":" + valueStr).getBytes(StandardCharsets.UTF_8);
    };

    public static final Deserializer<PersistedProperty> VALUE_DESERIALIZER = (topic, data) -> {
        String encoded = new String(data, StandardCharsets.UTF_8);
        String[] parts = encoded.split(":", 3);
        String[] values = parts[2].split(",");
        return new PersistedProperty(Long.parseLong(parts[0]), parts[1], Arrays.asList(values));
    };
    public static final Serde<PersistedProperty> SERDE = new Serde<PersistedProperty>() {
        @Override
        public Serializer<PersistedProperty> serializer() {
            return VALUE_SERIALIZER;
        }

        @Override
        public Deserializer<PersistedProperty> deserializer() {
            return VALUE_DESERIALIZER;
        }
    };

    private final long id;

    private final String property;

    private final List<String> values;

    public PersistedProperty(long id, String property, List<String> values) {
        this.id = id;
        this.property = property;
        this.values = values;
    }

    public static String keyFor(long id, String propertyName) {
        return id + "," + propertyName;
    }

    public long getId() {
        return id;
    }

    public String getKey() {
        return keyFor(id, property);
    }

    public String getProperty() {
        return property;
    }

    public List<String> getValues() {
        return values;
    }

    @Override
    public String toString() {
        return "PersistedProperty{" +
                "id=" + id +
                ", property='" + property + '\'' +
                ", values='" + values + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PersistedProperty that)) return false;

        if (id != that.id) return false;
        if (!Objects.equals(property, that.property)) return false;
        return Objects.equals(values, that.values);
    }

    @Override
    public int hashCode() {
        int result = (int) (id ^ (id >>> 32));
        result = 31 * result + (property != null ? property.hashCode() : 0);
        result = 31 * result + (values != null ? values.hashCode() : 0);
        return result;
    }

}
