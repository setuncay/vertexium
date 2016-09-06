package org.vertexium.sql.models;

import org.vertexium.Direction;

public abstract class EdgeInOutHiddenValue extends VertexTableEdgeValueBase {
    private static final long serialVersionUID = 1643548891701159692L;

    public EdgeInOutHiddenValue(String edgeId) {
        super(edgeId);
    }

    public abstract Direction getDirection();
}
