package org.vertexium.event;

import org.vertexium.Element;
import org.vertexium.Graph;
import org.vertexium.Property;
import org.vertexium.Visibility;
import org.vertexium.mutation.PropertyDeleteMutation;

public class DeletePropertyEvent extends GraphEvent {
    private final Element element;
    private final String key;
    private final String name;
    private final Visibility visibility;

    public DeletePropertyEvent(Graph graph, Element element, Property property) {
        super(graph);
        this.element = element;
        this.key = property.getKey();
        this.name = property.getName();
        this.visibility = property.getVisibility();
    }

    public DeletePropertyEvent(Graph graph, Element element, PropertyDeleteMutation propertyDeleteMutation) {
        super(graph);
        this.element = element;
        this.key = propertyDeleteMutation.getKey();
        this.name = propertyDeleteMutation.getName();
        this.visibility = propertyDeleteMutation.getVisibility();
    }

    public Element getElement() {
        return element;
    }

    public String getKey() {
        return key;
    }

    public String getName() {
        return name;
    }

    public Visibility getVisibility() {
        return visibility;
    }

    @Override
    public int hashCode() {
        return getKey().hashCode() ^ getName().hashCode() ^ getVisibility().hashCode();
    }

    @Override
    public String toString() {
        return "DeletePropertyEvent{element=" + getElement() + ", property=" + getKey() + ":" + getName() + ":" + getVisibility() + '}';
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof DeletePropertyEvent)) {
            return false;
        }

        DeletePropertyEvent other = (DeletePropertyEvent) obj;
        return getElement().equals(other.getElement())
                && getKey().equals(other.getKey())
                && getName().equals(other.getName())
                && getVisibility().equals(other.getVisibility());
    }
}
