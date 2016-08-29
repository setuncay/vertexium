package org.vertexium.sql;

import org.vertexium.*;
import org.vertexium.mutation.ExistingEdgeMutation;
import org.vertexium.mutation.PropertyDeleteMutation;
import org.vertexium.mutation.PropertySoftDeleteMutation;

import java.util.EnumSet;

public class SqlEdge extends SqlElement implements Edge {
    private final String outVertexId;
    private final String inVertexId;
    private final String label;

    public SqlEdge(
            Graph graph,
            String id,
            String outVertexId,
            String inVertexId,
            String label,
            String newEdgeLabel,
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
        this.outVertexId = outVertexId;
        this.inVertexId = inVertexId;
        this.label = label;
    }

    @Override
    public String getLabel() {
        return label;
    }

    @Override
    public String getVertexId(Direction direction) {
        switch (direction) {
            case OUT:
                return outVertexId;
            case IN:
                return inVertexId;
            default:
                throw new IllegalArgumentException("direction can only be IN or OUT");
        }
    }

    @Override
    public Vertex getVertex(Direction direction, Authorizations authorizations) {
        return getVertex(direction, FetchHint.ALL, authorizations);
    }

    @Override
    public Vertex getVertex(Direction direction, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        return getGraph().getVertex(getVertexId(direction), fetchHints, authorizations);
    }

    @Override
    public String getOtherVertexId(String myVertexId) {
        if (inVertexId.equals(myVertexId)) {
            return outVertexId;
        } else if (outVertexId.equals(myVertexId)) {
            return inVertexId;
        }
        throw new VertexiumException("myVertexId(" + myVertexId + ") does not appear on edge (" + getId() + ") in either the in (" + inVertexId + ") or the out (" + outVertexId + ").");
    }

    @Override
    public Vertex getOtherVertex(String myVertexId, Authorizations authorizations) {
        return getOtherVertex(myVertexId, FetchHint.ALL, authorizations);
    }

    @Override
    public Vertex getOtherVertex(String myVertexId, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        return getGraph().getVertex(getOtherVertexId(myVertexId), fetchHints, authorizations);
    }

    @SuppressWarnings("unchecked")
    @Override
    public ExistingEdgeMutation prepareMutation() {
        return new ExistingEdgeMutation(this) {
            @Override
            public Edge save(Authorizations authorizations) {
                getGraph().saveExistingElementMutation(this, authorizations);
                return getElement();
            }
        };
    }
}
