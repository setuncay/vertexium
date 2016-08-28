package org.vertexium.sql.models;

import org.vertexium.Visibility;

public abstract class EdgeInOutVisibleValue extends SqlGraphValueBase {
    private static final long serialVersionUID = 3279237608503657664L;
    private final String edgeId;
    private final Visibility visibility;

    public EdgeInOutVisibleValue(String edgeId, Visibility visibility) {
        this.edgeId = edgeId;
        this.visibility = visibility;
    }

    public String getEdgeId() {
        return edgeId;
    }

    public Visibility getVisibility() {
        return visibility;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "{" +
                "edgeId='" + edgeId + '\'' +
                "visibility='" + visibility + '\'' +
                '}';
    }
}
