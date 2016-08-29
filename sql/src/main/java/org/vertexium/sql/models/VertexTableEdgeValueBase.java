package org.vertexium.sql.models;

public abstract class VertexTableEdgeValueBase extends SqlGraphValueBase {
    private static final long serialVersionUID = -3165923882089724281L;
    private final String edgeId;

    protected VertexTableEdgeValueBase(String edgeId) {
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
