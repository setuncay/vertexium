package org.vertexium.sql.models;

import org.vertexium.Property;
import org.vertexium.Visibility;

public class PropertyHiddenValue extends PropertyValueBase {
    private static final long serialVersionUID = 5066940505205641023L;
    private final long timestamp;
    private final Visibility hiddenVisibility;

    public PropertyHiddenValue(Property property, long timestamp, Visibility hiddenVisibility) {
        this(
                property.getKey(),
                property.getName(),
                property.getTimestamp(),
                property.getVisibility(),
                timestamp,
                hiddenVisibility
        );
    }

    public PropertyHiddenValue(
            String propertyKey,
            String propertyName,
            long propertyTimestamp,
            Visibility propertyVisibility,
            long timestamp,
            Visibility hiddenVisibility
    ) {
        super(propertyKey, propertyName, propertyTimestamp, propertyVisibility);
        this.timestamp = timestamp;
        this.hiddenVisibility = hiddenVisibility;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Visibility getHiddenVisibility() {
        return hiddenVisibility;
    }

    @Override
    public String toString() {
        return "PropertyHiddenValue{" +
                "propertyKey='" + getPropertyKey() + '\'' +
                ", propertyName='" + getPropertyName() + '\'' +
                ", propertyVisibility=" + getPropertyVisibility() +
                ", propertyTimestamp=" + getPropertyTimestamp() +
                ", timestamp=" + getTimestamp() +
                ", hiddenVisibility=" + getHiddenVisibility() +
                "}";
    }
}
