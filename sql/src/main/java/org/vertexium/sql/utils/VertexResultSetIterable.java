package org.vertexium.sql.utils;

import org.vertexium.*;
import org.vertexium.mutation.PropertyDeleteMutation;
import org.vertexium.mutation.PropertySoftDeleteMutation;
import org.vertexium.sql.SqlGraph;
import org.vertexium.sql.SqlGraphSql;
import org.vertexium.sql.SqlVertex;
import org.vertexium.sql.models.*;

import java.util.*;

public class VertexResultSetIterable extends ElementResultSetIterable<Vertex> {
    public VertexResultSetIterable(
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
    protected Vertex createElement(String id, List<SqlGraphValueBase> values) {
        VertexSignalValue vertexSignalValue = (VertexSignalValue) getElementSignalValue(values);
        if (vertexSignalValue == null) {
            return null;
        }
        Collection<Property> properties = getProperties(values);
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
        Set<String> edgeIdsToRemove = new HashSet<>();
        for (SqlGraphValueBase value : values) {
            if (value instanceof EdgeInfoValue && ((EdgeInfoValue) value).getDirection() == direction) {
                final EdgeInfoValue v = (EdgeInfoValue) value;
                edgeInfos.add(new DefaultEdgeInfo(v.getEdgeId(), v.getEdgeLabel(), v.getOtherVertexId()));
            } else if (value instanceof SoftDeleteInOutEdgeValue) {
                SoftDeleteInOutEdgeValue v = (SoftDeleteInOutEdgeValue) value;
                for (int i = edgeInfos.size() - 1; i >= 0; i--) {
                    EdgeInfo edgeInfo = edgeInfos.get(i);
                    if (edgeInfo.getEdgeId().equals(v.getEdgeId())) {
                        edgeInfos.remove(i);
                    }
                }
            } else if (value instanceof EdgeInOutHiddenValue) {
                edgeIdsToRemove.add(((EdgeInOutHiddenValue) value).getEdgeId());
            } else if (value instanceof EdgeInOutVisibleValue) {
                edgeIdsToRemove.remove(((EdgeInOutVisibleValue) value).getEdgeId());
            }
        }

        for (String edgeIdToRemove : edgeIdsToRemove) {
            for (int i = edgeInfos.size() - 1; i >= 0; i--) {
                EdgeInfo edgeInfo = edgeInfos.get(i);
                if (edgeInfo.getEdgeId().equals(edgeIdToRemove)) {
                    edgeInfos.remove(i);
                }
            }
        }

        return edgeInfos;
    }
}
