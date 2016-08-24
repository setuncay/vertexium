package org.vertexium.sql.models;

import org.vertexium.Visibility;

public class PropertyMetadataValue extends PropertyValueBase {
    private static final long serialVersionUID = -3301706159732174712L;
    private final Object value;
    private final String key;
    private final Visibility visibility;

    public PropertyMetadataValue(
            String propertyKey,
            String propertyName,
            long propertyTimestamp,
            Visibility propertyVisibility,
            String key,
            Object value,
            Visibility visibility
    ) {
        super(propertyKey, propertyName, propertyTimestamp, propertyVisibility);
        this.key = key;
        this.value = value;
        this.visibility = visibility;
    }

    public Object getValue() {
        return value;
    }

    public String getKey() {
        return key;
    }

    public Visibility getVisibility() {
        return visibility;
    }

    @Override
    public String toString() {
        return "PropertyValueValue{" +
                "propertyKey='" + getPropertyKey() + '\'' +
                ", propertyName='" + getPropertyName() + '\'' +
                ", propertyVisibility=" + getPropertyVisibility() +
                ", propertyTimestamp=" + getPropertyTimestamp() +
                ", key=" + getKey() +
                ", visibility=" + getVisibility() +
                '}';
    }
}
