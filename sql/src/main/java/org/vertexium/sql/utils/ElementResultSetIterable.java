package org.vertexium.sql.utils;

import org.vertexium.*;
import org.vertexium.mutation.KeyNameVisibilityPropertySoftDeleteMutation;
import org.vertexium.mutation.PropertyDeleteMutation;
import org.vertexium.mutation.PropertySoftDeleteMutation;
import org.vertexium.property.MutablePropertyImpl;
import org.vertexium.security.ColumnVisibility;
import org.vertexium.security.VisibilityEvaluator;
import org.vertexium.security.VisibilityParseException;
import org.vertexium.sql.SqlElement;
import org.vertexium.sql.SqlGraph;
import org.vertexium.sql.SqlGraphSql;
import org.vertexium.sql.models.*;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public abstract class ElementResultSetIterable<T extends Element> extends SqlGraphSqlResultSetIterable<T> {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(ElementResultSetIterable.class);
    private final SqlGraph graph;
    private final EnumSet<FetchHint> fetchHints;
    private final Long endTime;
    private final VertexiumSerializer serializer;
    private final Authorizations authorizations;
    private final VisibilityEvaluator visibilityEvaluator;

    public ElementResultSetIterable(
            SqlGraphSql sqlGraphSql,
            SqlGraph graph,
            EnumSet<FetchHint> fetchHints,
            Long endTime,
            VertexiumSerializer serializer,
            Authorizations authorizations,
            PreparedStatementCreator preparedStatementCreator
    ) {
        super(sqlGraphSql, preparedStatementCreator);
        this.graph = graph;
        this.fetchHints = fetchHints;
        this.endTime = endTime;
        this.serializer = serializer;
        this.authorizations = authorizations;
        visibilityEvaluator = new VisibilityEvaluator(authorizations.getAuthorizations());
    }

    @Override
    protected T readFromResultSet(ResultSet rs) throws SQLException {
        if (rs.isBeforeFirst()) {
            if (!rs.next()) {
                return null;
            }
        }

        T elem;
        while (!rs.isAfterLast()) {
            elem = readElement(rs);
            if (elem != null) {
                return elem;
            }
        }
        return null;
    }

    private T readElement(ResultSet rs) throws SQLException {
        boolean first = true;
        String id = null;
        List<SqlGraphValueBase> values = new ArrayList<>();

        while (true) {
            if (!first && !rs.getString(SqlElement.COLUMN_ID).equals(id)) {
                break;
            }
            first = false;

            id = rs.getString(SqlElement.COLUMN_ID);
            String visibilityString = rs.getString(SqlElement.COLUMN_VISIBILITY);
            try {
                if (!visibilityEvaluator.evaluate(new ColumnVisibility(visibilityString))) {
                    if (!rs.next()) {
                        break;
                    }
                    continue;
                }
            } catch (VisibilityParseException e) {
                LOGGER.error("Could not parse visibility string: %s", visibilityString, e);
                if (!rs.next()) {
                    break;
                }
                continue;
            }

            byte[] valueBytes = rs.getBytes(SqlElement.COLUMN_VALUE);
            Object valueObject = serializer.bytesToObject(valueBytes);
            if (!(valueObject instanceof SqlGraphValueBase)) {
                throw new VertexiumException("Invalid valud object type: " + valueObject.getClass().getName());
            }
            SqlGraphValueBase value = (SqlGraphValueBase) valueObject;
            values.add(value);

            if (!rs.next()) {
                break;
            }
        }

        return createElement(id, values);
    }

    protected abstract T createElement(String id, List<SqlGraphValueBase> values);

    protected SqlGraph getGraph() {
        return graph;
    }

    protected Authorizations getAuthorizations() {
        return authorizations;
    }

    protected VertexiumSerializer getSerializer() {
        return serializer;
    }

    protected ElementSignalValueBase getElementSignalValue(List<SqlGraphValueBase> values) {
        boolean includeHidden = fetchHints.contains(FetchHint.INCLUDE_HIDDEN);

        if (!includeHidden) {
            Set<Visibility> hiddenVisibilities = new HashSet<>();
            for (SqlGraphValueBase value : values) {
                if (value instanceof ElementHiddenValue) {
                    hiddenVisibilities.add(((ElementHiddenValue) value).getVisibility());
                } else if (value instanceof ElementVisibleValue) {
                    hiddenVisibilities.remove(((ElementVisibleValue) value).getVisibility());
                }
            }
            if (hiddenVisibilities.size() > 0) {
                return null;
            }
        }

        ElementSignalValueBase result = null;
        for (SqlGraphValueBase value : values) {
            if (value instanceof ElementSignalValueBase) {
                result = (ElementSignalValueBase) value;
            } else if (value instanceof SoftDeleteElementValue) {
                result = null;
            }
        }
        return result;
    }

    protected Collection<Property> getProperties(List<SqlGraphValueBase> values) {
        Map<PropertyMapKey, Property> properties = new HashMap<>();
        Set<PropertyMapKey> propertyMapKeysToRemove = new HashSet<>();
        for (SqlGraphValueBase value : values) {
            if (value instanceof PropertyValueValue) {
                PropertyValueValue v = (PropertyValueValue) value;
                Metadata propertyMetadata = getPropertyMetadata(
                        values,
                        v.getPropertyKey(),
                        v.getPropertyName(),
                        v.getPropertyVisibility(),
                        v.getTimestamp()
                );

                Set<Visibility> propertyHiddenVisibilities = null;
                Object propertyValue = v.getValue();

                if (propertyValue instanceof SqlStreamingPropertyValueRef) {
                    SqlStreamingPropertyValueRef sspvr = (SqlStreamingPropertyValueRef) propertyValue;
                    Class valueType = sspvr.getValueType();
                    long length = sspvr.getLength();
                    propertyValue = new SqlStreamingPropertyValue(getGraph(), sspvr, valueType, length);
                }

                Property property = new MutablePropertyImpl(
                        v.getPropertyKey(),
                        v.getPropertyName(),
                        propertyValue,
                        propertyMetadata,
                        v.getTimestamp(),
                        propertyHiddenVisibilities,
                        v.getPropertyVisibility()
                );
                properties.put(new PropertyMapKey(property), property);
            } else if (value instanceof PropertyHiddenValue) {
                PropertyHiddenValue phv = (PropertyHiddenValue) value;
                applyHiddenToProperty(properties, phv, propertyMapKeysToRemove);
            } else if (value instanceof PropertyVisibleValue) {
                PropertyVisibleValue pvv = (PropertyVisibleValue) value;
                applyVisibleToProperty(properties, pvv, propertyMapKeysToRemove);
            }
        }

        for (PropertyMapKey propertyMapKey : propertyMapKeysToRemove) {
            properties.remove(propertyMapKey);
        }

        return properties.values();
    }

    private void applyHiddenToProperty(Map<PropertyMapKey, Property> properties, PropertyHiddenValue phv, Set<PropertyMapKey> propertyMapKeysToRemove) {
        boolean includeHidden = fetchHints.contains(FetchHint.INCLUDE_HIDDEN);
        PropertyMapKey propertyMapKey = new PropertyMapKey(phv);
        if (includeHidden) {
            Property property = properties.get(propertyMapKey);
            if (property != null) {
                ((MutablePropertyImpl) property).addHiddenVisibility(phv.getHiddenVisibility());
            }
        } else {
            propertyMapKeysToRemove.add(propertyMapKey);
        }
    }

    private void applyVisibleToProperty(Map<PropertyMapKey, Property> properties, PropertyVisibleValue pvv, Set<PropertyMapKey> propertyMapKeysToRemove) {
        PropertyMapKey propertyMapKey = new PropertyMapKey(pvv);
        Property property = properties.get(propertyMapKey);
        if (property != null) {
            ((MutablePropertyImpl) property).removeHiddenVisibility(pvv.getHiddenVisibility());
        }
        propertyMapKeysToRemove.remove(propertyMapKey);
    }

    private Metadata getPropertyMetadata(
            List<SqlGraphValueBase> values,
            String propertyKey,
            String propertyName,
            Visibility propertyVisibility,
            long timestamp
    ) {
        Metadata metadata = new Metadata();
        for (SqlGraphValueBase value : values) {
            if (!(value instanceof PropertyMetadataValue)) {
                continue;
            }
            PropertyMetadataValue pmv = (PropertyMetadataValue) value;
            if (pmv.getTimestamp() < timestamp) {
                continue;
            }
            if (!propertyKey.equals(pmv.getPropertyKey())) {
                continue;
            }
            if (!propertyName.equals(pmv.getPropertyName())) {
                continue;
            }
            if (!propertyVisibility.equals(pmv.getPropertyVisibility())) {
                continue;
            }
            metadata.add(pmv.getKey(), pmv.getValue(), pmv.getVisibility());
        }
        return metadata;
    }

    protected List<PropertyDeleteMutation> getPropertyDeleteMutation(List<SqlGraphValueBase> values) {
        // TODO
        return new ArrayList<>();
    }

    protected List<PropertySoftDeleteMutation> getPropertySoftDeleteMutation(
            List<SqlGraphValueBase> values,
            Collection<Property> properties
    ) {
        List<PropertySoftDeleteMutation> results = new ArrayList<>();
        for (SqlGraphValueBase value : values) {
            if (value instanceof PropertySoftDeleteValue) {
                PropertySoftDeleteValue psdv = (PropertySoftDeleteValue) value;
                if (isPropertyDefinedAfter(properties, psdv)) {
                    continue;
                }
                PropertySoftDeleteMutation psdm = new KeyNameVisibilityPropertySoftDeleteMutation(
                        psdv.getPropertyKey(),
                        psdv.getPropertyName(),
                        psdv.getPropertyVisibility()
                );
                results.add(psdm);
            }
        }
        return results;
    }

    private boolean isPropertyDefinedAfter(Collection<Property> properties, PropertySoftDeleteValue psdv) {
        for (Property property : properties) {
            if (property.getKey().equals(psdv.getPropertyKey())
                    && property.getName().equals(psdv.getPropertyName())
                    && property.getVisibility().equals(psdv.getPropertyVisibility())) {
                if (property.getTimestamp() > psdv.getTimestamp()) {
                    return true;
                }
            }
        }
        return false;
    }

    protected List<Visibility> getHiddenVisibilities(List<SqlGraphValueBase> values) {
        List<Visibility> results = new ArrayList<>();
        for (SqlGraphValueBase value : values) {
            if (value instanceof ElementHiddenValue) {
                ElementHiddenValue ehv = (ElementHiddenValue) value;
                results.add(ehv.getVisibility());
            }
        }
        return results;
    }

    @SuppressWarnings("unchecked")
    public static <T extends Element> ElementResultSetIterable<T> create(
            SqlGraphSql sqlGraphSql,
            SqlGraph graph,
            ElementType elementType,
            EnumSet<FetchHint> fetchHints,
            Long endTime,
            VertexiumSerializer serializer,
            Authorizations authorizations,
            PreparedStatementCreator preparedStatementCreator
    ) {
        switch (elementType) {
            case VERTEX:
                return (ElementResultSetIterable<T>) new VertexResultSetIterable(sqlGraphSql, graph, fetchHints, endTime, serializer, authorizations, preparedStatementCreator);
            case EDGE:
                return (ElementResultSetIterable<T>) new EdgeResultSetIterable(sqlGraphSql, graph, fetchHints, endTime, serializer, authorizations, preparedStatementCreator);
            default:
                throw new VertexiumException("unexpected element type: " + elementType);
        }
    }

    private static class PropertyMapKey {
        private final String propertyKey;
        private final String propertyName;
        private final Visibility propertyVisibility;

        public PropertyMapKey(Property property) {
            this.propertyKey = property.getKey();
            this.propertyName = property.getName();
            this.propertyVisibility = property.getVisibility();
        }

        public PropertyMapKey(PropertyValueBase propertyValueBase) {
            this.propertyKey = propertyValueBase.getPropertyKey();
            this.propertyName = propertyValueBase.getPropertyName();
            this.propertyVisibility = propertyValueBase.getPropertyVisibility();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            PropertyMapKey that = (PropertyMapKey) o;

            if (!propertyKey.equals(that.propertyKey)) {
                return false;
            }
            if (!propertyName.equals(that.propertyName)) {
                return false;
            }
            if (!propertyVisibility.equals(that.propertyVisibility)) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            int result = propertyKey.hashCode();
            result = 31 * result + propertyName.hashCode();
            result = 31 * result + propertyVisibility.hashCode();
            return result;
        }
    }
}
