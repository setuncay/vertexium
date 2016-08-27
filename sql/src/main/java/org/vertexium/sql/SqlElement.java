package org.vertexium.sql;

import org.vertexium.*;
import org.vertexium.mutation.ExistingElementMutation;
import org.vertexium.mutation.ExistingElementMutationImpl;
import org.vertexium.mutation.PropertyDeleteMutation;
import org.vertexium.mutation.PropertySoftDeleteMutation;

import java.util.ArrayList;
import java.util.List;

public class SqlElement extends ElementBase {
    public static final String COLUMN_VALUE = "value";
    public static final String COLUMN_VISIBILITY = "visibility";
    public static final String COLUMN_TYPE = "type";
    public static final String COLUMN_TIMESTAMP = "timestamp";
    public static final String COLUMN_ID = "key";

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
        Property property = super.softDeletePropertyInternal(key, name);
        if (property != null) {
            List<Property> properties = new ArrayList<>();
            properties.add(property);
            getGraph().softDeleteProperties(this, properties, null, authorizations);
        }
    }

    @Override
    public void softDeleteProperty(String key, String name, Visibility visibility, Authorizations authorizations) {
        Property property = super.softDeletePropertyInternal(key, name, visibility);
        if (property != null) {
            List<Property> properties = new ArrayList<>();
            properties.add(property);
            getGraph().softDeleteProperties(this, properties, null, authorizations);
        }
    }

    @Override
    public void softDeleteProperties(String name, Authorizations authorizations) {
        Iterable<Property> properties = super.removePropertyInternal(name);
        getGraph().softDeleteProperties(this, properties, null, authorizations);
    }

    @Override
    public <TElement extends Element> ExistingElementMutation<TElement> prepareMutation() {
        TElement elem = (TElement) this;
        return new ExistingElementMutationImpl<TElement>(elem) {
            @Override
            public TElement save(Authorizations authorizations) {
                getGraph().saveExistingElementMutation(this, authorizations);
                return getElement();
            }
        };
    }

    @Override
    public void markPropertyHidden(Property property, Long timestamp, Visibility visibility, Authorizations authorizations) {
        getGraph().markPropertyHidden(this, property, timestamp, visibility);
    }

    @Override
    public void markPropertyVisible(Property property, Long timestamp, Visibility visibility, Authorizations authorizations) {
        getGraph().markPropertyVisible(this, property, timestamp, visibility);
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

    @Override
    public Iterable<HistoricalPropertyValue> getHistoricalPropertyValues(
            String key,
            String name,
            Visibility visibility,
            Long startTime,
            Long endTime,
            Authorizations authorizations
    ) {
        return getGraph().getHistoricalPropertyValues(this, key, name, visibility, startTime, endTime, authorizations);
    }
}
