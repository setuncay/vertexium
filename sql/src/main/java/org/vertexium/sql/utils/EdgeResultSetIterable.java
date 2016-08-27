package org.vertexium.sql.utils;

import org.vertexium.*;
import org.vertexium.mutation.PropertyDeleteMutation;
import org.vertexium.mutation.PropertySoftDeleteMutation;
import org.vertexium.sql.SqlEdge;
import org.vertexium.sql.SqlGraph;
import org.vertexium.sql.SqlGraphSQL;
import org.vertexium.sql.models.EdgeSignalValue;
import org.vertexium.sql.models.SqlGraphValueBase;

import java.util.Collection;
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
    protected Edge createElement(String id, List<SqlGraphValueBase> values) {
        EdgeSignalValue edgeSignalValue = (EdgeSignalValue) getElementSignalValue(values);
        if (edgeSignalValue == null) {
            return null;
        }
        String newEdgeLabel = null;
        Collection<Property> properties = getProperties(values);
        List<PropertyDeleteMutation> propertyDeleteMutations = getPropertyDeleteMutation(values);
        List<PropertySoftDeleteMutation> propertySoftDeleteMutations = getPropertySoftDeleteMutation(values, properties);
        List<Visibility> hiddenVisibilities = getHiddenVisibilities(values);

        return new SqlEdge(
                getGraph(),
                id,
                edgeSignalValue.getOutVertexId(),
                edgeSignalValue.getInVertexId(),
                edgeSignalValue.getEdgeLabel(),
                newEdgeLabel,
                edgeSignalValue.getVisibility(),
                properties,
                propertyDeleteMutations,
                propertySoftDeleteMutations,
                hiddenVisibilities,
                edgeSignalValue.getTimestamp(),
                getAuthorizations()
        );
    }
}
