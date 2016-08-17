package org.vertexium.sql.utils;

import org.vertexium.*;
import org.vertexium.mutation.PropertyDeleteMutation;
import org.vertexium.mutation.PropertySoftDeleteMutation;
import org.vertexium.sql.SqlEdge;
import org.vertexium.sql.SqlGraph;
import org.vertexium.sql.SqlGraphSQL;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.List;

public abstract class EdgeResultSetIterable extends ElementResultSetIterable<Edge> {
    public EdgeResultSetIterable(
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
    protected Edge createElement(
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
        String newEdgeLabel = null;

        return new SqlEdge(
                getGraph(),
                id,
                outVertexId,
                inVertexId,
                label,
                newEdgeLabel,
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
        return getSerializer().bytesToObject(rs.getBytes(SqlEdge.COLUMN_VALUE));
    }

    @Override
    protected String readEdgeOutVertexIdFromSignalRow(ResultSet rs) throws SQLException {
        return rs.getString(SqlEdge.COLUMN_OUT_VERTEX_ID);
    }

    @Override
    protected String readEdgeInVertexIdFromSignalRow(ResultSet rs) throws SQLException {
        return rs.getString(SqlEdge.COLUMN_IN_VERTEX_ID);
    }
}
