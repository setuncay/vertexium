package org.vertexium.sql.models;

import org.vertexium.Property;
import org.vertexium.Visibility;

public class PropertySoftDeleteValue extends PropertyValueBase {
    private static final long serialVersionUID = -6949369646320113988L;

    public PropertySoftDeleteValue(Property property) {
        this(
                property.getKey(),
                property.getName(),
                property.getTimestamp(),
                property.getVisibility()
        );
    }


    public PropertySoftDeleteValue(String key, String name, long timestamp, Visibility visibility) {
        super(key, name, timestamp, visibility);
    }

    @Override
    public String toString() {
        return "PropertySoftDeleteValue{" +
                "propertyKey='" + getPropertyKey() + '\'' +
                ", propertyName='" + getPropertyName() + '\'' +
                ", propertyVisibility=" + getPropertyVisibility() +
                ", propertyTimestamp=" + getPropertyTimestamp() +
                "}";
    }
}
