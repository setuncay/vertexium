package org.vertexium.sql.models;

public abstract class SoftDeleteInOutEdgeValue extends VertexTableEdgeValueBase {
    private static final long serialVersionUID = -9217509679777334473L;

    protected SoftDeleteInOutEdgeValue(String edgeId) {
        super(edgeId);
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "{" +
                "edgeId='" + getEdgeId() + '\'' +
                '}';
    }
}
