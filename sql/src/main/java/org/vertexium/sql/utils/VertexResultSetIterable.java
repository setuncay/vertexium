package org.vertexium.sql.utils;

import org.vertexium.*;
import org.vertexium.sql.SqlGraph;
import org.vertexium.sql.SqlGraphSql;
import org.vertexium.sql.SqlVertex;
import org.vertexium.sql.models.*;

import java.util.*;

public class VertexResultSetIterable extends ElementResultSetIterable<Vertex> {
    private final List<EdgeInfo> outEdgeInfos = new ArrayList<>();
    private final Set<String> outEdgeIdsToRemove = new HashSet<>();
    private final List<EdgeInfo> inEdgeInfos = new ArrayList<>();
    private final Set<String> inEdgeIdsToRemove = new HashSet<>();

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
    protected void clear() {
        super.clear();
        outEdgeInfos.clear();
        inEdgeInfos.clear();
        outEdgeIdsToRemove.clear();
    }

    @Override
    protected Vertex createElement(String id) {
        removeEdgeInfos(this.outEdgeInfos, this.outEdgeIdsToRemove);
        removeEdgeInfos(this.inEdgeInfos, this.inEdgeIdsToRemove);

        return new SqlVertex(
                getGraph(),
                id,
                elementSignalValue.getVisibility(),
                properties.values(),
                propertyDeleteMutations.values(),
                propertySoftDeleteMutations.values(),
                hiddenVisibilities,
                elementSignalValue.getTimestamp(),
                outEdgeInfos,
                inEdgeInfos,
                getAuthorizations()
        );
    }

    private void removeEdgeInfos(List<EdgeInfo> edgeInfos, Set<String> idsToRemove) {
        for (String outEdgeIdToRemove : idsToRemove) {
            for (int i = edgeInfos.size() - 1; i >= 0; i--) {
                EdgeInfo edgeInfo = edgeInfos.get(i);
                if (edgeInfo.getEdgeId().equals(outEdgeIdToRemove)) {
                    edgeInfos.remove(i);
                }
            }
        }
    }

    @Override
    protected void addValue(SqlGraphValueBase value) {
        super.addValue(value);

        if (value instanceof EdgeInfoValue) {
            EdgeInfoValue v = (EdgeInfoValue) value;
            List<EdgeInfo> edgeInfos = getEdgeInfos(v.getDirection());
            edgeInfos.add(new DefaultEdgeInfo(v.getEdgeId(), v.getEdgeLabel(), v.getOtherVertexId()));
        } else if (value instanceof SoftDeleteInOutEdgeValue) {
            SoftDeleteInOutEdgeValue v = (SoftDeleteInOutEdgeValue) value;
            List<EdgeInfo> edgeInfos = getEdgeInfos(v.getDirection());
            for (int i = edgeInfos.size() - 1; i >= 0; i--) {
                EdgeInfo edgeInfo = edgeInfos.get(i);
                if (edgeInfo.getEdgeId().equals(v.getEdgeId())) {
                    edgeInfos.remove(i);
                }
            }
        } else if (!includeHidden && value instanceof EdgeInOutHiddenValue) {
            EdgeInOutHiddenValue v = (EdgeInOutHiddenValue) value;
            Set<String> edgeIdsToRemove = getEdgeIdsToRemove(v.getDirection());
            edgeIdsToRemove.add(((EdgeInOutHiddenValue) value).getEdgeId());
        } else if (!includeHidden && value instanceof EdgeInOutVisibleValue) {
            EdgeInOutVisibleValue v = (EdgeInOutVisibleValue) value;
            Set<String> edgeIdsToRemove = getEdgeIdsToRemove(v.getDirection());
            edgeIdsToRemove.remove(((EdgeInOutVisibleValue) value).getEdgeId());
        }
    }

    private Set<String> getEdgeIdsToRemove(Direction direction) {
        switch (direction) {
            case OUT:
                return outEdgeIdsToRemove;
            case IN:
                return inEdgeIdsToRemove;
            default:
                throw new VertexiumException("Unhandled direction: " + direction);
        }
    }

    private List<EdgeInfo> getEdgeInfos(Direction direction) {
        switch (direction) {
            case OUT:
                return outEdgeInfos;
            case IN:
                return inEdgeInfos;
            default:
                throw new VertexiumException("Unhandled direction: " + direction);
        }
    }
}
