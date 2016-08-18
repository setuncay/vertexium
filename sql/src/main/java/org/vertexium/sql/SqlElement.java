package org.vertexium.sql;

import org.vertexium.*;
import org.vertexium.mutation.ExistingElementMutation;
import org.vertexium.mutation.PropertyDeleteMutation;
import org.vertexium.mutation.PropertySoftDeleteMutation;

public class SqlElement extends ElementBase {
    public static final String COLUMN_VALUE = "value";
    public static final String COLUMN_VISIBILITY = "visibility";
    public static final String COLUMN_TYPE = "type";
    public static final String COLUMN_TIMESTAMP = "timestamp";
    public static final String COLUMN_ID = "key";
    public static final String COLUMN_PROPERTY_KEY = "pkey";
    public static final String COLUMN_PROPERTY_NAME = "pname";

    protected SqlElement(
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
    public void deleteProperty(String key, String name, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public void deleteProperties(String name, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public void softDeleteProperty(String key, String name, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public void softDeleteProperty(String key, String name, Visibility visibility, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public void softDeleteProperties(String name, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public <T extends Element> ExistingElementMutation<T> prepareMutation() {
        throw new VertexiumException("not implemented");
    }

    @Override
    public void markPropertyHidden(Property property, Long timestamp, Visibility visibility, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public void markPropertyVisible(Property property, Long timestamp, Visibility visibility, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public void deleteProperty(String key, String name, Visibility visibility, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    protected void updatePropertiesInternal(
            Iterable<Property> properties,
            Iterable<PropertyDeleteMutation> propertyDeleteMutations,
            Iterable<PropertySoftDeleteMutation> propertySoftDeleteMutations
    ) {
        super.updatePropertiesInternal(properties, propertyDeleteMutations, propertySoftDeleteMutations);
    }

    @Override
    public SqlGraph getGraph() {
        return (SqlGraph) super.getGraph();
    }
}
