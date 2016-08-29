package org.vertexium.sql.models;

import org.vertexium.Visibility;

public abstract class EdgeInOutVisibleValue extends VertexTableEdgeValueBase {
    private static final long serialVersionUID = 3279237608503657664L;
    private final Visibility visibility;

    public EdgeInOutVisibleValue(String edgeId, Visibility visibility) {
        super(edgeId);
        this.visibility = visibility;
    }

    public Visibility getVisibility() {
        return visibility;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "{" +
                "edgeId='" + getEdgeId() + '\'' +
                "visibility='" + visibility + '\'' +
                '}';
    }
}
