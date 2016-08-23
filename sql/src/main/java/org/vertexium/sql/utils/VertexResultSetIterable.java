package org.vertexium.sql.utils;

import org.vertexium.*;
import org.vertexium.mutation.PropertyDeleteMutation;
import org.vertexium.mutation.PropertySoftDeleteMutation;
import org.vertexium.sql.SqlGraph;
import org.vertexium.sql.SqlGraphSQL;
import org.vertexium.sql.SqlVertex;
import org.vertexium.sql.models.SqlGraphValueBase;
import org.vertexium.sql.models.VertexSignalValue;

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
    protected Vertex createElement(String id, List<SqlGraphValueBase> values) {
        VertexSignalValue vertexSignalValue = (VertexSignalValue) getElementSignalValue(values);
        List<Property> properties = getProperties(values);
        List<PropertyDeleteMutation> propertyDeleteMutations = getPropertyDeleteMutation(values);
        List<PropertySoftDeleteMutation> propertySoftDeleteMutations = getPropertySoftDeleteMutation(values);
        List<Visibility> hiddenVisibilities = getHiddenVisibilities(values);

        return new SqlVertex(
                getGraph(),
                id,
                vertexSignalValue.getVisibility(),
                properties,
                propertyDeleteMutations,
                propertySoftDeleteMutations,
                hiddenVisibilities,
                vertexSignalValue.getTimestamp(),
                getAuthorizations()
        );
    }
}
