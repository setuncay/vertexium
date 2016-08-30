package org.vertexium.sql.models;

import org.vertexium.Metadata;
import org.vertexium.Property;
import org.vertexium.Visibility;

public class PropertyMetadataValue extends PropertyValueBase {
    private static final long serialVersionUID = -3301706159732174712L;
    private final Object value;
    private final String key;
    private final Visibility visibility;
    private final long timestamp;

    public PropertyMetadataValue(
            String propertyKey,
            String propertyName,
            Visibility propertyVisibility,
            String key,
            Object value,
            Visibility visibility,
            long timestamp
    ) {
        super(propertyKey, propertyName, propertyVisibility);
        this.key = key;
        this.value = value;
        this.visibility = visibility;
        this.timestamp = timestamp;
    }

    public PropertyMetadataValue(Property property, Metadata.Entry metadataEntry, long timestamp) {
        this(
                property.getKey(),
                property.getName(),
                property.getVisibility(),
                metadataEntry.getKey(),
                metadataEntry.getValue(),
                metadataEntry.getVisibility(),
                timestamp
        );
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

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "PropertyValueValue{" +
                "propertyKey='" + getPropertyKey() + '\'' +
                ", propertyName='" + getPropertyName() + '\'' +
                ", propertyVisibility=" + getPropertyVisibility() +
                ", key=" + getKey() +
                ", visibility=" + getVisibility() +
                ", timestamp=" + getTimestamp() +
                '}';
    }
}
