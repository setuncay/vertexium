package org.vertexium.sql.models;

import org.vertexium.Direction;

public class EdgeOutHiddenValue extends EdgeInOutHiddenValue {
    private static final long serialVersionUID = -1882898628054416037L;

    public EdgeOutHiddenValue(String edgeId) {
        super(edgeId);
    }

    @Override
    public Direction getDirection() {
        return Direction.OUT;
    }
}
