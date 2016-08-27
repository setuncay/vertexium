package org.vertexium.sql.models;

import org.vertexium.Property;
import org.vertexium.Visibility;

public class PropertySoftDeleteValue extends PropertyValueBase {
    private static final long serialVersionUID = -6949369646320113988L;
    private final long timestamp;

    public PropertySoftDeleteValue(Property property, long timestamp) {
        this(
                property.getKey(),
                property.getName(),
                property.getTimestamp(),
                property.getVisibility(),
                timestamp
        );
    }


    public PropertySoftDeleteValue(String key, String name, long propertyTimestamp, Visibility visibility, long timestamp) {
        super(key, name, propertyTimestamp, visibility);
        this.timestamp = timestamp;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "PropertySoftDeleteValue{" +
                "propertyKey='" + getPropertyKey() + '\'' +
                ", propertyName='" + getPropertyName() + '\'' +
                ", propertyVisibility=" + getPropertyVisibility() +
                ", propertyTimestamp=" + getPropertyTimestamp() +
                ", timestamp=" + getTimestamp() +
                "}";
    }
}
