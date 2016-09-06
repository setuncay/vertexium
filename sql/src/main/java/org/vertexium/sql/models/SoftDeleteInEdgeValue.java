package org.vertexium.sql.models;

import org.vertexium.Direction;

public class SoftDeleteInEdgeValue extends SoftDeleteInOutEdgeValue {
    private static final long serialVersionUID = -9113558063292523912L;

    public SoftDeleteInEdgeValue(String edgeId) {
        super(edgeId);
    }

    @Override
    public Direction getDirection() {
        return Direction.IN;
    }
}
