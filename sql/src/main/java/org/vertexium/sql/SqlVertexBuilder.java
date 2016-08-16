package org.vertexium.sql;

import org.vertexium.Authorizations;
import org.vertexium.VertexBuilder;
import org.vertexium.Visibility;

public abstract class SqlVertexBuilder extends VertexBuilder {
    public SqlVertexBuilder(String vertexId, Visibility visibility) {
        super(vertexId, visibility);
    }

    protected abstract SqlVertex createVertex(Authorizations authorizations);
}
