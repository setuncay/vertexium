package org.vertexium.sql.models;

import org.vertexium.Visibility;

public class PropertyValueValue extends PropertyValueBase {
    private static final long serialVersionUID = -1482874143756938428L;
    private final long timestamp;
    private final Object value;

    public PropertyValueValue(
            String propertyKey,
            String propertyName,
            long timestamp,
            Object value,
            Visibility propertyVisibility
    ) {
        super(propertyKey, propertyName, propertyVisibility);
        this.timestamp = timestamp;
        this.value = value;
    }

    public Object getValue() {
        return value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "PropertyValueValue{" +
                "propertyKey='" + getPropertyKey() + '\'' +
                ", propertyName='" + getPropertyName() + '\'' +
                ", propertyVisibility=" + getPropertyVisibility() +
                ", timestamp=" + getTimestamp() +
                '}';
    }
}
