package org.vertexium.sql.models;

import org.vertexium.Direction;
import org.vertexium.Visibility;

public abstract class EdgeInOutVisibleValue extends VertexTableEdgeValueBase {
    private static final long serialVersionUID = 3279237608503657664L;
    private final String visibility;

    public EdgeInOutVisibleValue(String edgeId, Visibility visibility) {
        super(edgeId);
        this.visibility = visibility.getVisibilityString();
    }

    public Visibility getVisibility() {
        return new Visibility(visibility);
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "{" +
                "edgeId='" + getEdgeId() + '\'' +
                "visibility='" + visibility + '\'' +
                '}';
    }

    public abstract Direction getDirection();
}
