package cc.dont_panic.experiments.kafka.data;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class PersistedProperty {

    public static final Serializer<PersistedProperty> VALUE_SERIALIZER = (topic, data) -> {
        return (data.id + ":" + data.property + ":" + data.value).getBytes(StandardCharsets.UTF_8);
    };

    public static final Deserializer<PersistedProperty> VALUE_DESERIALIZER = (topic, data) -> {
        String encoded = new String(data, StandardCharsets.UTF_8);
        String[] parts = encoded.split(":", 3);
        return new PersistedProperty(Long.parseLong(parts[0]), parts[1], parts[2]);
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

    private final String value;

    public PersistedProperty(long id, String property, String value) {
        this.id = id;
        this.property = property;
        this.value = value;
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

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "PersistedProperty{" +
                "id=" + id +
                ", property='" + property + '\'' +
                ", value='" + value + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PersistedProperty that)) return false;

        if (id != that.id) return false;
        if (!Objects.equals(property, that.property)) return false;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        int result = (int) (id ^ (id >>> 32));
        result = 31 * result + (property != null ? property.hashCode() : 0);
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }

}
