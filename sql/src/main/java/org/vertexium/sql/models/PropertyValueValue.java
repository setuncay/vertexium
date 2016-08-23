package org.vertexium.sql.models;

import org.vertexium.Visibility;

public class PropertyValueValue extends PropertyValueBase {
    private static final long serialVersionUID = -1482874143756938428L;
    private final Object value;

    public PropertyValueValue(
            String propertyKey,
            String propertyName,
            long propertyTimestamp,
            Object value,
            Visibility propertyVisibility
    ) {
        super(propertyKey, propertyName, propertyTimestamp, propertyVisibility);
        this.value = value;
    }

    public Object getValue() {
        return value;
    }
}
