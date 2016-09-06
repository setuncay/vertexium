package org.vertexium.sql.models;

import org.vertexium.Direction;
import org.vertexium.Visibility;

public class EdgeInfoValue extends VertexTableEdgeValueBase {
    private static final long serialVersionUID = -57036505856770487L;
    private final Direction direction;
    private final String edgeLabel;
    private final String edgeVisibility;
    private final String otherVertexId;

    public EdgeInfoValue(
            Direction direction,
            String edgeId,
            String edgeLabel,
            String otherVertexId,
            Visibility edgeVisibility
    ) {
        super(edgeId);
        this.direction = direction;
        this.edgeLabel = edgeLabel;
        this.edgeVisibility = edgeVisibility.getVisibilityString();
        this.otherVertexId = otherVertexId;
    }

    public Direction getDirection() {
        return direction;
    }

    public String getEdgeLabel() {
        return edgeLabel;
    }

    public Visibility getEdgeVisibility() {
        return new Visibility(edgeVisibility);
    }

    public String getOtherVertexId() {
        return otherVertexId;
    }

    @Override
    public String toString() {
        return "EdgeInfoValue{" +
                "edgeId='" + getEdgeId() + '\'' +
                ", edgeLabel='" + getEdgeLabel() + '\'' +
                ", edgeVisibility=" + getEdgeVisibility() +
                ", otherVertexId='" + getOtherVertexId() + '\'' +
                ", direction=" + getDirection() +
                '}';
    }
}
