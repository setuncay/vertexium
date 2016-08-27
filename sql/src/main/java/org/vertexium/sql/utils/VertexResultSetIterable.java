package org.vertexium.sql.utils;

import org.vertexium.*;
import org.vertexium.mutation.PropertyDeleteMutation;
import org.vertexium.mutation.PropertySoftDeleteMutation;
import org.vertexium.sql.SqlGraph;
import org.vertexium.sql.SqlGraphSQL;
import org.vertexium.sql.SqlVertex;
import org.vertexium.sql.models.EdgeInfoValue;
import org.vertexium.sql.models.SqlGraphValueBase;
import org.vertexium.sql.models.VertexSignalValue;

import java.util.ArrayList;
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
        if (vertexSignalValue == null) {
            return null;
        }
        List<Property> properties = getProperties(values);
        List<PropertyDeleteMutation> propertyDeleteMutations = getPropertyDeleteMutation(values);
        List<PropertySoftDeleteMutation> propertySoftDeleteMutations = getPropertySoftDeleteMutation(values, properties);
        List<Visibility> hiddenVisibilities = getHiddenVisibilities(values);
        List<EdgeInfo> outEdgeInfos = getEdgeInfos(values, Direction.OUT);
        List<EdgeInfo> inEdgeInfos = getEdgeInfos(values, Direction.IN);

        return new SqlVertex(
                getGraph(),
                id,
                vertexSignalValue.getVisibility(),
                properties,
                propertyDeleteMutations,
                propertySoftDeleteMutations,
                hiddenVisibilities,
                vertexSignalValue.getTimestamp(),
                outEdgeInfos,
                inEdgeInfos,
                getAuthorizations()
        );
    }

    private List<EdgeInfo> getEdgeInfos(List<SqlGraphValueBase> values, Direction direction) {
        List<EdgeInfo> edgeInfos = new ArrayList<>();
        for (SqlGraphValueBase value : values) {
            if (value instanceof EdgeInfoValue && ((EdgeInfoValue) value).getDirection() == direction) {
                final EdgeInfoValue v = (EdgeInfoValue) value;
                edgeInfos.add(new EdgeInfo() {
                    @Override
                    public String getEdgeId() {
                        return v.getEdgeId();
                    }

                    @Override
                    public String getLabel() {
                        return v.getEdgeLabel();
                    }

                    @Override
                    public String getVertexId() {
                        return v.getOtherVertexId();
                    }
                });
            }
        }
        return edgeInfos;
    }
}
