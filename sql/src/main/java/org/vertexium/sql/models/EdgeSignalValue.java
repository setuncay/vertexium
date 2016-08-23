package org.vertexium.sql.models;

import org.vertexium.Visibility;

public class EdgeSignalValue extends ElementSignalValueBase {
    private static final long serialVersionUID = -664736084238732325L;
    private final String edgeLabel;
    private final String outVertexId;
    private final String inVertexId;

    public EdgeSignalValue(long timestamp, String edgeLabel, String outVertexId, String inVertexId, Visibility visibility) {
        super(timestamp, visibility);
        this.edgeLabel = edgeLabel;
        this.outVertexId = outVertexId;
        this.inVertexId = inVertexId;
    }

    public String getEdgeLabel() {
        return edgeLabel;
    }

    public String getOutVertexId() {
        return outVertexId;
    }

    public String getInVertexId() {
        return inVertexId;
    }
}
