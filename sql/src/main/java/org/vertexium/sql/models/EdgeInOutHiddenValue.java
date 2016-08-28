package org.vertexium.sql.models;

public abstract class EdgeInOutHiddenValue extends SqlGraphValueBase {
    private static final long serialVersionUID = 1643548891701159692L;
    private final String edgeId;

    public EdgeInOutHiddenValue(String edgeId) {
        this.edgeId = edgeId;
    }

    public String getEdgeId() {
        return edgeId;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "{" +
                "edgeId='" + edgeId + '\'' +
                '}';
    }
}
