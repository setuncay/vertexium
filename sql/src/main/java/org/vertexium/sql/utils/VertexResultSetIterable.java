package org.vertexium.sql.utils;

import org.vertexium.*;
import org.vertexium.mutation.PropertyDeleteMutation;
import org.vertexium.mutation.PropertySoftDeleteMutation;
import org.vertexium.sql.SqlGraph;
import org.vertexium.sql.SqlGraphSQL;
import org.vertexium.sql.SqlVertex;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.List;

public abstract class VertexResultSetIterable extends ElementResultSetIterable<Vertex> {
    public VertexResultSetIterable(
            SqlGraphSQL sqlGraphSQL,
            SqlGraph graph,
            EnumSet<FetchHint> fetchHints,
            Long endTime,
            VertexiumSerializer serializer,
            Authorizations authorizations
    ) {
        super(sqlGraphSQL, graph, fetchHints, endTime, serializer, authorizations);
    }

    @Override
    protected Vertex createElement(
            String id,
            String outVertexId,
            String inVertexId,
            String label,
            Visibility visibility,
            Long timestamp,
            List<Property> properties,
            List<PropertyDeleteMutation> propertyDeleteMutations,
            List<PropertySoftDeleteMutation> propertySoftDeleteMutations,
            List<Visibility> hiddenVisibilities
    ) {
        return new SqlVertex(
                getGraph(),
                id,
                visibility,
                properties,
                propertyDeleteMutations,
                propertySoftDeleteMutations,
                hiddenVisibilities,
                timestamp,
                getAuthorizations()
        );
    }

    @Override
    protected String readEdgeLabelFromSignalRow(ResultSet rs) throws SQLException {
        return null;
    }

    @Override
    protected String readEdgeOutVertexIdFromSignalRow(ResultSet rs) throws SQLException {
        return null;
    }

    @Override
    protected String readEdgeInVertexIdFromSignalRow(ResultSet rs) throws SQLException {
        return null;
    }
}
