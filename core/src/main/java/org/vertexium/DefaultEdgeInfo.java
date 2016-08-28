package org.vertexium;

public class DefaultEdgeInfo implements EdgeInfo {
    private final String edgeId;
    private final String label;
    private final String vertexId;

    public DefaultEdgeInfo(String edgeId, String label, String vertexId) {
        this.edgeId = edgeId;
        this.label = label;
        this.vertexId = vertexId;
    }

    @Override
    public String getEdgeId() {
        return edgeId;
    }

    @Override
    public String getLabel() {
        return label;
    }

    @Override
    public String getVertexId() {
        return vertexId;
    }
}
