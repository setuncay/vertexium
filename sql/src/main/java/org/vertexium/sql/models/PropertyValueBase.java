package org.vertexium.sql.models;

import org.vertexium.Visibility;

public class PropertyValueBase extends SqlGraphValueBase {
    private static final long serialVersionUID = 6380908379560535511L;
    private final String propertyKey;
    private final String propertyName;
    private final Visibility propertyVisibility;

    public PropertyValueBase(
            String propertyKey,
            String propertyName,
            Visibility propertyVisibility
    ) {
        this.propertyKey = propertyKey;
        this.propertyName = propertyName;
        this.propertyVisibility = propertyVisibility;
    }

    public String getPropertyKey() {
        return propertyKey;
    }

    public String getPropertyName() {
        return propertyName;
    }


    public Visibility getPropertyVisibility() {
        return propertyVisibility;
    }

    @Override
    public String toString() {
        return "PropertyValueBase{" +
                "propertyKey='" + getPropertyKey() + '\'' +
                ", propertyName='" + getPropertyName() + '\'' +
                ", propertyVisibility=" + getPropertyVisibility() +
                '}';
    }
}