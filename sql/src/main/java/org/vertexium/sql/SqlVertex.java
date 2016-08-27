package org.vertexium.sql;

import org.vertexium.*;
import org.vertexium.mutation.ExistingElementMutation;
import org.vertexium.mutation.ExistingElementMutationImpl;
import org.vertexium.mutation.PropertyDeleteMutation;
import org.vertexium.mutation.PropertySoftDeleteMutation;
import org.vertexium.query.VertexQuery;
import org.vertexium.util.ConvertingIterable;
import org.vertexium.util.FilterIterable;
import org.vertexium.util.JoinIterable;
import org.vertexium.util.LookAheadIterable;

import java.util.*;

public class SqlVertex extends SqlElement implements Vertex {
    private final List<EdgeInfo> outEdgeInfos;
    private final List<EdgeInfo> inEdgeInfos;

    public SqlVertex(
            Graph graph,
            String id,
            Visibility visibility,
            Iterable<Property> properties,
            Iterable<PropertyDeleteMutation> propertyDeleteMutations,
            Iterable<PropertySoftDeleteMutation> propertySoftDeleteMutations,
            Iterable<Visibility> hiddenVisibilities,
            long timestamp,
            Authorizations authorizations
    ) {
        this(
                graph,
                id,
                visibility,
                properties,
                propertyDeleteMutations,
                propertySoftDeleteMutations,
                hiddenVisibilities,
                timestamp,
                new ArrayList<EdgeInfo>(),
                new ArrayList<EdgeInfo>(),
                authorizations
        );
    }

    public SqlVertex(
            Graph graph,
            String id,
            Visibility visibility,
            Iterable<Property> properties,
            Iterable<PropertyDeleteMutation> propertyDeleteMutations,
            Iterable<PropertySoftDeleteMutation> propertySoftDeleteMutations,
            Iterable<Visibility> hiddenVisibilities,
            long timestamp,
            List<EdgeInfo> outEdgeInfos,
            List<EdgeInfo> inEdgeInfos,
            Authorizations authorizations
    ) {
        super(
                graph,
                id,
                visibility,
                properties,
                propertyDeleteMutations,
                propertySoftDeleteMutations,
                hiddenVisibilities,
                timestamp,
                authorizations
        );
        this.outEdgeInfos = outEdgeInfos;
        this.inEdgeInfos = inEdgeInfos;
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, Authorizations authorizations) {
        return getEdges(direction, FetchHint.ALL, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        return getEdges(direction, fetchHints, null, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, EnumSet<FetchHint> fetchHints, Long endTime, Authorizations authorizations) {
        Iterable<String> edgeIds = getEdgeIds(direction, authorizations);
        return getGraph().getEdges(edgeIds, fetchHints, endTime, authorizations);
    }

    @Override
    public Iterable<String> getEdgeIds(Direction direction, Authorizations authorizations) {
        return getEdgeIds(direction, (String[]) null, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, String label, Authorizations authorizations) {
        return getEdges(direction, new String[]{label}, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, String label, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        return getEdges(direction, new String[]{label}, fetchHints, authorizations);
    }

    @Override
    public Iterable<String> getEdgeIds(Direction direction, String label, Authorizations authorizations) {
        return getEdgeIds(direction, new String[]{label}, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, String[] labels, Authorizations authorizations) {
        return getEdges(direction, labels, FetchHint.ALL, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, String[] labels, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        Iterable<String> edgeIds = getEdgeIds(direction, labels, authorizations);
        return getGraph().getEdges(edgeIds, fetchHints, null, authorizations);
    }

    @Override
    public Iterable<String> getEdgeIds(Direction direction, String[] labels, Authorizations authorizations) {
        return new ConvertingIterable<EdgeInfo, String>(getEdgeInfos(direction, labels, authorizations)) {
            @Override
            protected String convert(EdgeInfo edgeInfo) {
                return edgeInfo.getEdgeId();
            }
        };
    }

    @Override
    public Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, Authorizations authorizations) {
        return getEdges(otherVertex, direction, FetchHint.ALL, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        Iterable<String> edgeIds = getEdgeIds(otherVertex, direction, authorizations);
        return getGraph().getEdges(edgeIds, fetchHints, authorizations);
    }

    @Override
    public Iterable<String> getEdgeIds(Vertex otherVertex, Direction direction, Authorizations authorizations) {
        return getEdgeIds(otherVertex, direction, (String[]) null, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, String label, Authorizations authorizations) {
        return getEdges(otherVertex, direction, new String[]{label}, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, String label, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        return getEdges(otherVertex, direction, new String[]{label}, fetchHints, authorizations);
    }

    @Override
    public Iterable<String> getEdgeIds(Vertex otherVertex, Direction direction, String label, Authorizations authorizations) {
        return getEdgeIds(otherVertex, direction, new String[]{label}, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, String[] labels, Authorizations authorizations) {
        return getEdges(otherVertex, direction, labels, FetchHint.ALL, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, String[] labels, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        Iterable<String> edgeIds = getEdgeIds(otherVertex, direction, labels, authorizations);
        return getGraph().getEdges(edgeIds, fetchHints, authorizations);
    }

    @Override
    public Iterable<String> getEdgeIds(final Vertex otherVertex, Direction direction, String[] labels, Authorizations authorizations) {
        final Iterable<EdgeInfo> edgeInfos = getEdgeInfos(direction, labels, authorizations);
        return new LookAheadIterable<EdgeInfo, String>() {
            @Override
            protected boolean isIncluded(EdgeInfo edgeInfo, String edgeId) {
                return edgeInfo.getVertexId().equals(otherVertex.getId());
            }

            @Override
            protected String convert(EdgeInfo edgeInfo) {
                return edgeInfo.getEdgeId();
            }

            @Override
            protected Iterator<EdgeInfo> createIterator() {
                return edgeInfos.iterator();
            }
        };
    }

    @Override
    public int getEdgeCount(Direction direction, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public Iterable<String> getEdgeLabels(Direction direction, Authorizations authorizations) {
        Set<String> edgeLabels = new HashSet<>();
        for (EdgeInfo edgeInfo : getEdgeInfos(direction, authorizations)) {
            edgeLabels.add(edgeInfo.getLabel());
        }
        return edgeLabels;
    }

    @Override
    public Iterable<EdgeInfo> getEdgeInfos(Direction direction, Authorizations authorizations) {
        switch (direction) {
            case IN:
                return inEdgeInfos;
            case OUT:
                return outEdgeInfos;
            case BOTH:
                return new JoinIterable<>(inEdgeInfos, outEdgeInfos);
            default:
                throw new VertexiumException("Unexpected direction: " + direction);
        }
    }

    @Override
    public Iterable<EdgeInfo> getEdgeInfos(Direction direction, String label, Authorizations authorizations) {
        return getEdgeInfos(direction, new String[]{label}, authorizations);
    }

    @Override
    public Iterable<EdgeInfo> getEdgeInfos(Direction direction, final String[] labels, Authorizations authorizations) {
        return new FilterIterable<EdgeInfo>(getEdgeInfos(direction, authorizations)) {
            @Override
            protected boolean isIncluded(EdgeInfo edgeInfo) {
                return isEdgeInfoIncluded(edgeInfo, labels);
            }
        };
    }

    private boolean isEdgeInfoIncluded(EdgeInfo edgeInfo, String[] labels) {
        if (labels == null) {
            return true;
        }
        for (String label : labels) {
            if (edgeInfo.getLabel().equals(label)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, Authorizations authorizations) {
        return getVertices(direction, FetchHint.ALL, authorizations);
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        return getVertices(direction, (String[]) null, fetchHints, authorizations);
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String label, Authorizations authorizations) {
        return getVertices(direction, label, FetchHint.ALL, authorizations);
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String label, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        return getVertices(direction, new String[]{label}, fetchHints, authorizations);
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String[] labels, Authorizations authorizations) {
        return getVertices(direction, labels, FetchHint.ALL, authorizations);
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String[] labels, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        Iterable<String> vertexIds = getVertexIds(direction, labels, authorizations);
        return getGraph().getVertices(vertexIds, fetchHints, authorizations);
    }

    @Override
    public Iterable<String> getVertexIds(Direction direction, String label, Authorizations authorizations) {
        return getVertexIds(direction, new String[]{label}, authorizations);
    }

    @Override
    public Iterable<String> getVertexIds(final Direction direction, final String[] labels, final Authorizations authorizations) {
        return new LookAheadIterable<EdgeInfo, String>() {
            @Override
            protected boolean isIncluded(EdgeInfo edgeInfo, String vertexId) {
                return isEdgeInfoIncluded(edgeInfo, labels);
            }

            @Override
            protected String convert(EdgeInfo edgeInfo) {
                return edgeInfo.getVertexId();
            }

            @Override
            protected Iterator<EdgeInfo> createIterator() {
                return getEdgeInfos(direction, labels, authorizations).iterator();
            }
        };
    }

    @Override
    public Iterable<String> getVertexIds(Direction direction, Authorizations authorizations) {
        return getVertexIds(direction, (String[]) null, authorizations);
    }

    @Override
    public VertexQuery query(Authorizations authorizations) {
        return query(null, authorizations);
    }

    @Override
    public VertexQuery query(String queryString, Authorizations authorizations) {
        return getGraph().getSearchIndex().queryVertex(getGraph(), this, queryString, authorizations);
    }

    @SuppressWarnings("unchecked")
    @Override
    public ExistingElementMutation<Vertex> prepareMutation() {
        return new ExistingElementMutationImpl<Vertex>(this) {
            @Override
            public Vertex save(Authorizations authorizations) {
                getGraph().saveExistingElementMutation(this, authorizations);
                return getElement();
            }
        };
    }

    @Override
    public Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, Authorizations authorizations) {
        return getEdgeVertexPairs(getEdgeInfos(direction, authorizations), FetchHint.ALL, null, authorizations);
    }

    @Override
    public Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        return getEdgeVertexPairs(getEdgeInfos(direction, authorizations), fetchHints, null, authorizations);
    }

    @Override
    public Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, EnumSet<FetchHint> fetchHints, Long endTime, Authorizations authorizations) {
        return getEdgeVertexPairs(getEdgeInfos(direction, authorizations), fetchHints, endTime, authorizations);
    }

    @Override
    public Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, String label, Authorizations authorizations) {
        return getEdgeVertexPairs(getEdgeInfos(direction, label, authorizations), FetchHint.ALL, null, authorizations);
    }

    @Override
    public Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, String label, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        return getEdgeVertexPairs(getEdgeInfos(direction, label, authorizations), fetchHints, null, authorizations);
    }

    @Override
    public Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, String[] labels, Authorizations authorizations) {
        return getEdgeVertexPairs(getEdgeInfos(direction, labels, authorizations), FetchHint.ALL, null, authorizations);
    }

    @Override
    public Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, String[] labels, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        return getEdgeVertexPairs(getEdgeInfos(direction, labels, authorizations), fetchHints, null, authorizations);
    }

    private Iterable<EdgeVertexPair> getEdgeVertexPairs(Iterable<EdgeInfo> edgeInfos, EnumSet<FetchHint> fetchHints, Long endTime, Authorizations authorizations) {
        return EdgeVertexPair.getEdgeVertexPairs(getGraph(), getId(), edgeInfos, fetchHints, endTime, authorizations);
    }

    protected void addOutEdgeInfo(final Edge outEdge) {
        addOutEdgeInfo(createEdgeInfoFromEdge(outEdge));
    }

    protected EdgeInfo createEdgeInfoFromEdge(final Edge outEdge) {
        return new EdgeInfo() {
            @Override
            public String getEdgeId() {
                return outEdge.getId();
            }

            @Override
            public String getLabel() {
                return outEdge.getLabel();
            }

            @Override
            public String getVertexId() {
                return outEdge.getOtherVertexId(SqlVertex.this.getId());
            }
        };
    }

    protected void addOutEdgeInfo(EdgeInfo outEdgeInfo) {
        outEdgeInfos.add(outEdgeInfo);
    }

    protected void addInEdgeInfo(Edge inEdge) {
        addInEdgeInfo(createEdgeInfoFromEdge(inEdge));
    }

    protected void addInEdgeInfo(EdgeInfo inEdgeInfo) {
        inEdgeInfos.add(inEdgeInfo);
    }
}
