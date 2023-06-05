package cc.dont_panic.experiments.kafka.data;

import java.util.Objects;

public class ChangeRequest {

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
