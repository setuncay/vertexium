package org.vertexium.sql.models;

import org.vertexium.Visibility;

public class ElementVisibleValue extends SqlGraphValueBase {
    private static final long serialVersionUID = 8375800506743394075L;
    private final Visibility visibility;

    public ElementVisibleValue(Visibility visibility) {
        this.visibility = visibility;
    }

    public Visibility getVisibility() {
        return visibility;
    }

    @Override
    public String toString() {
        return "ElementVisibleValue{" +
                "visibility=" + getVisibility() +
                "}";
    }
}
