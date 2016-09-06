package org.vertexium.sql.models;

import org.vertexium.Property;
import org.vertexium.Visibility;

public class PropertyVisibleValue extends PropertyValueBase {
    private static final long serialVersionUID = 7261634601706920158L;
    private final long timestamp;
    private final String hiddenVisibility;

    public PropertyVisibleValue(Property property, long timestamp, Visibility hiddenVisibility) {
        this(
                property.getKey(),
                property.getName(),
                property.getVisibility(),
                timestamp,
                hiddenVisibility
        );
    }

    public PropertyVisibleValue(
            String propertyKey,
            String propertyName,
            Visibility propertyVisibility,
            long timestamp,
            Visibility hiddenVisibility
    ) {
        super(propertyKey, propertyName, propertyVisibility);
        this.timestamp = timestamp;
        this.hiddenVisibility = hiddenVisibility.getVisibilityString();
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Visibility getHiddenVisibility() {
        return new Visibility(hiddenVisibility);
    }

    @Override
    public String toString() {
        return "PropertyVisibleValue{" +
                "propertyKey='" + getPropertyKey() + '\'' +
                ", propertyName='" + getPropertyName() + '\'' +
                ", propertyVisibility=" + getPropertyVisibility() +
                ", timestamp=" + getTimestamp() +
                ", hiddenVisibility=" + getHiddenVisibility() +
                "}";
    }
}
