package org.vertexium.sql.models;

import org.vertexium.Direction;

public class EdgeInHiddenValue extends EdgeInOutHiddenValue {
    private static final long serialVersionUID = 3713780334500488724L;

    public EdgeInHiddenValue(String edgeId) {
        super(edgeId);
    }

    @Override
    public Direction getDirection() {
        return Direction.IN;
    }
}
