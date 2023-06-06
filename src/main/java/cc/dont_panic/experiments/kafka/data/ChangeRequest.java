package cc.dont_panic.experiments.kafka.data;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class ChangeRequest {

    public static final Serializer<ChangeRequest> VALUE_SERIALIZER = (topic, data) -> {
        String serializableString = data.id + ":" + data.propertyName + ":" + data.propertyValue;
        return serializableString.getBytes(StandardCharsets.UTF_8);
    };
    public static final Deserializer<ChangeRequest> VALUE_DESERIALIZER = (topic, bytes) -> {
        String serializedString = new String(bytes, StandardCharsets.UTF_8);
        String[] parts = serializedString.split(":");
        return new ChangeRequest(Long.parseLong(parts[0]), parts[1], parts[2]);
    };
    public static final Serde<ChangeRequest> SERDE = new Serde<ChangeRequest>() {
        @Override
        public Serializer<ChangeRequest> serializer() {
            return VALUE_SERIALIZER;
        }

        @Override
        public Deserializer<ChangeRequest> deserializer() {
            return VALUE_DESERIALIZER;
        }
    };

    private final long id;
    private final String propertyName;
    private final String propertyValue;

    public ChangeRequest(long id, String propertyName, String propertyValue) {
        this.id = id;
        this.propertyName = propertyName;
        this.propertyValue = propertyValue;
    }

    public long getId() {
        return id;
    }

    public String getPropertyName() {
        return propertyName;
    }

    public String getPropertyValue() {
        return propertyValue;
    }

    @Override
    public String toString() {
        return "ChangeRequest{" +
                "id=" + id +
                ", propertyName='" + propertyName + '\'' +
                ", propertyValue='" + propertyValue + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ChangeRequest that)) return false;

        if (id != that.id) return false;
        if (!Objects.equals(propertyName, that.propertyName)) return false;
        return Objects.equals(propertyValue, that.propertyValue);
    }

    @Override
    public int hashCode() {
        int result = (int) (id ^ (id >>> 32));
        result = 31 * result + (propertyName != null ? propertyName.hashCode() : 0);
        result = 31 * result + (propertyValue != null ? propertyValue.hashCode() : 0);
        return result;
    }
}
