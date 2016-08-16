package org.vertexium.sql;

import org.vertexium.*;
import org.vertexium.mutation.ExistingElementMutation;
import org.vertexium.mutation.PropertyDeleteMutation;
import org.vertexium.mutation.PropertySoftDeleteMutation;
import org.vertexium.query.VertexQuery;

import java.util.EnumSet;

public class SqlVertex extends SqlElement implements Vertex {
    protected SqlVertex(
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
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, EnumSet<FetchHint> fetchHints, Long endTime, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public Iterable<String> getEdgeIds(Direction direction, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, String label, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, String label, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public Iterable<String> getEdgeIds(Direction direction, String label, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, String[] labels, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, String[] labels, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public Iterable<String> getEdgeIds(Direction direction, String[] labels, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public Iterable<String> getEdgeIds(Vertex otherVertex, Direction direction, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, String label, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, String label, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public Iterable<String> getEdgeIds(Vertex otherVertex, Direction direction, String label, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, String[] labels, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, String[] labels, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public Iterable<String> getEdgeIds(Vertex otherVertex, Direction direction, String[] labels, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public int getEdgeCount(Direction direction, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public Iterable<String> getEdgeLabels(Direction direction, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public Iterable<EdgeInfo> getEdgeInfos(Direction direction, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public Iterable<EdgeInfo> getEdgeInfos(Direction direction, String label, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public Iterable<EdgeInfo> getEdgeInfos(Direction direction, String[] labels, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String label, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String label, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String[] labels, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String[] labels, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public Iterable<String> getVertexIds(Direction direction, String label, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public Iterable<String> getVertexIds(Direction direction, String[] labels, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public Iterable<String> getVertexIds(Direction direction, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public VertexQuery query(Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public VertexQuery query(String queryString, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public ExistingElementMutation<Vertex> prepareMutation() {
        throw new VertexiumException("not implemented");
    }

    @Override
    public Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, EnumSet<FetchHint> fetchHints, Long endTime, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, String label, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, String label, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, String[] labels, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, String[] labels, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }
}
