package org.vertexium.sql.models;

public abstract class SoftDeleteInOutEdgeValue extends SqlGraphValueBase {
    private static final long serialVersionUID = -9217509679777334473L;
    private final String edgeId;

    protected SoftDeleteInOutEdgeValue(String edgeId) {
        this.edgeId = edgeId;
    }

    public String getEdgeId() {
        return edgeId;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "{" +
                "edgeId='" + getEdgeId() + '\'' +
                '}';
    }
}
