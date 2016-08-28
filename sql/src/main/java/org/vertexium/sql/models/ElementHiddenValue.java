package org.vertexium.sql.models;

import org.vertexium.Visibility;

public class ElementHiddenValue extends SqlGraphValueBase {
    private static final long serialVersionUID = -1850142361575904766L;
    private final Visibility visibility;

    public ElementHiddenValue(Visibility visibility) {
        this.visibility = visibility;
    }

    public Visibility getVisibility() {
        return visibility;
    }

    @Override
    public String toString() {
        return "ElementHiddenValue{" +
                "visibility=" + getVisibility() +
                "}";
    }
}
