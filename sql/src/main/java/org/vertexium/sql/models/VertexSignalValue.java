package org.vertexium.sql.models;

import org.vertexium.Visibility;

public class VertexSignalValue extends ElementSignalValueBase {
    private static final long serialVersionUID = 6295632305848910921L;

    public VertexSignalValue(long timestamp, Visibility visibility) {
        super(timestamp, visibility);
    }

    @Override
    public String toString() {
        return "VertexSignalValue{" +
                "visibility=" + getVisibility() +
                ", timestamp=" + getTimestamp() +
                "}";
    }
}
