package org.vertexium;

import org.vertexium.event.AddPropertyEvent;
import org.vertexium.event.AddVertexEvent;
import org.vertexium.event.DeletePropertyEvent;
import org.vertexium.event.GraphEvent;
import org.vertexium.mutation.PropertyDeleteMutation;

public abstract class VertexBuilder extends ElementBuilder<Vertex> {
    private String vertexId;
    private Visibility visibility;

    public VertexBuilder(String vertexId, Visibility visibility) {
        this.vertexId = vertexId;
        this.visibility = visibility;
    }

    /**
     * Save the vertex along with any properties that were set to the graph.
     *
     * @return The newly created vertex.
     */
    @Override
    public abstract Vertex save(Authorizations authorizations);

    public String getVertexId() {
        return vertexId;
    }

    public Visibility getVisibility() {
        return visibility;
    }

    protected void notifyEventListeners(Graph graph, Vertex vertex) {
        queueEvent(new AddVertexEvent(graph, vertex));
        for (Property property : getProperties()) {
            queueEvent(new AddPropertyEvent(graph, vertex, property));
        }
        for (PropertyDeleteMutation propertyDeleteMutation : getPropertyDeletes()) {
            queueEvent(new DeletePropertyEvent(graph, vertex, propertyDeleteMutation));
        }
    }

    protected abstract void queueEvent(GraphEvent event);
}
