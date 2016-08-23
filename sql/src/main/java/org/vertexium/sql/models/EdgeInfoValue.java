package org.vertexium.sql.models;

import org.vertexium.Direction;
import org.vertexium.Visibility;

public class EdgeInfoValue extends SqlGraphValueBase {
    private static final long serialVersionUID = -57036505856770487L;
    private final Direction direction;
    private final String edgeId;
    private final String edgeLabel;
    private final Visibility edgeVisibility;
    private final String otherVertexId;

    public EdgeInfoValue(
            Direction direction,
            String edgeId,
            String edgeLabel,
            String otherVertexId,
            Visibility edgeVisibility
    ) {
        this.direction = direction;
        this.edgeId = edgeId;
        this.edgeLabel = edgeLabel;
        this.edgeVisibility = edgeVisibility;
        this.otherVertexId = otherVertexId;
    }

    public Direction getDirection() {
        return direction;
    }

    public String getEdgeId() {
        return edgeId;
    }

    public String getEdgeLabel() {
        return edgeLabel;
    }

    public Visibility getEdgeVisibility() {
        return edgeVisibility;
    }

    public String getOtherVertexId() {
        return otherVertexId;
    }
}
