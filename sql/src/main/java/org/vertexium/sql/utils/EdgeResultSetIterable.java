package org.vertexium.sql.utils;

import org.vertexium.Authorizations;
import org.vertexium.Edge;
import org.vertexium.FetchHint;
import org.vertexium.VertexiumSerializer;
import org.vertexium.sql.SqlEdge;
import org.vertexium.sql.SqlGraph;
import org.vertexium.sql.SqlGraphSql;
import org.vertexium.sql.models.EdgeSignalValue;

import java.util.EnumSet;

public class EdgeResultSetIterable extends ElementResultSetIterable<Edge> {
    private String newEdgeLabel;

    public EdgeResultSetIterable(
            SqlGraphSql sqlGraphSql,
            SqlGraph graph,
            EnumSet<FetchHint> fetchHints,
            Long endTime,
            VertexiumSerializer serializer,
            Authorizations authorizations,
            PreparedStatementCreator preparedStatementCreator
    ) {
        super(sqlGraphSql, graph, fetchHints, endTime, serializer, authorizations, preparedStatementCreator);
    }

    @Override
    protected void clear() {
        super.clear();
        newEdgeLabel = null;
    }

    @Override
    protected Edge createElement(String id) {
        return new SqlEdge(
                getGraph(),
                id,
                ((EdgeSignalValue) elementSignalValue).getOutVertexId(),
                ((EdgeSignalValue) elementSignalValue).getInVertexId(),
                ((EdgeSignalValue) elementSignalValue).getEdgeLabel(),
                newEdgeLabel,
                elementSignalValue.getVisibility(),
                properties.values(),
                propertyDeleteMutations.values(),
                propertySoftDeleteMutations.values(),
                hiddenVisibilities,
                elementSignalValue.getTimestamp(),
                getAuthorizations()
        );
    }
}
