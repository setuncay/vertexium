package org.vertexium.sql.models;

import org.vertexium.Direction;

public class SoftDeleteOutEdgeValue extends SoftDeleteInOutEdgeValue {
    private static final long serialVersionUID = 6451195099021586707L;

    public SoftDeleteOutEdgeValue(String edgeId) {
        super(edgeId);
    }

    @Override
    public Direction getDirection() {
        return Direction.OUT;
    }
}
