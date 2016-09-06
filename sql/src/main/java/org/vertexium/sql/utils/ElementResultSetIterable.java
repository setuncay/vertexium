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
    protected ElementSignalValueBase elementSignalValue = null;
    protected final Map<PropertyMapKey, PropertyDeleteMutation> propertyDeleteMutations = new HashMap<>();
    protected final Map<PropertyMapKey, PropertySoftDeleteMutation> propertySoftDeleteMutations = new HashMap<>();
    protected final Map<PropertyMapKey, Property> properties = new HashMap<>();
    protected final Set<PropertyMapKey> propertyMapKeysToRemove = new HashSet<>();
    protected final Map<PropertyMapKey, Metadata> propertyMetadatas = new HashMap<>();
    protected final List<Visibility> hiddenVisibilities = new ArrayList<>();

    protected final boolean includeHidden;

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
        includeHidden = fetchHints.contains(FetchHint.INCLUDE_HIDDEN);
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
        clear();

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
                throw new VertexiumException("Invalid valid object type: " + valueObject.getClass().getName());
            }
            SqlGraphValueBase value = (SqlGraphValueBase) valueObject;
            addValue(value);

            if (!rs.next()) {
                break;
            }
        }

        if (!includeHidden && hiddenVisibilities.size() > 0) {
            return null;
        }

        if (elementSignalValue == null) {
            return null;
        }

        for (PropertyMapKey propertyMapKey : propertyMapKeysToRemove) {
            properties.remove(propertyMapKey);
        }

        return createElement(id);
    }

    protected void clear() {
        elementSignalValue = null;
        properties.clear();
        propertyMapKeysToRemove.clear();
        propertyMetadatas.clear();
        propertyDeleteMutations.clear();
        propertySoftDeleteMutations.clear();
        hiddenVisibilities.clear();
    }

    protected void addValue(SqlGraphValueBase value) {
        if (value instanceof ElementHiddenValue) {
            hiddenVisibilities.add(((ElementHiddenValue) value).getVisibility());
        } else if (value instanceof ElementVisibleValue) {
            hiddenVisibilities.remove(((ElementVisibleValue) value).getVisibility());
        } else if (value instanceof ElementSignalValueBase) {
            elementSignalValue = (ElementSignalValueBase) value;
        } else if (value instanceof SoftDeleteElementValue) {
            elementSignalValue = null;
        } else if (value instanceof PropertySoftDeleteValue) {
            PropertySoftDeleteValue propertySoftDeleteValue = (PropertySoftDeleteValue) value;
            PropertyMapKey propertyMapKey = new PropertyMapKey(propertySoftDeleteValue);
            PropertySoftDeleteMutation propertySoftDeleteMutation = new KeyNameVisibilityPropertySoftDeleteMutation(
                    propertySoftDeleteValue.getPropertyKey(),
                    propertySoftDeleteValue.getPropertyName(),
                    propertySoftDeleteValue.getPropertyVisibility()
            );
            propertySoftDeleteMutations.put(propertyMapKey, propertySoftDeleteMutation);
            properties.remove(propertyMapKey);
            propertyMetadatas.remove(propertyMapKey);
        } else if (value instanceof PropertyValueValue) {
            PropertyValueValue v = (PropertyValueValue) value;
            PropertyMapKey propertyMapKey = new PropertyMapKey(v);
            Metadata propertyMetadata = propertyMetadatas.get(propertyMapKey);
            if (propertyMetadata == null) {
                propertyMetadata = new Metadata();
                propertyMetadatas.put(propertyMapKey, propertyMetadata);
            }

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
            properties.put(propertyMapKey, property);
            propertySoftDeleteMutations.remove(propertyMapKey);
        } else if (value instanceof PropertyHiddenValue) {
            PropertyHiddenValue phv = (PropertyHiddenValue) value;
            applyHiddenToProperty(properties, phv, propertyMapKeysToRemove);
        } else if (value instanceof PropertyVisibleValue) {
            PropertyVisibleValue pvv = (PropertyVisibleValue) value;
            applyVisibleToProperty(properties, pvv, propertyMapKeysToRemove);
        } else if (value instanceof PropertyMetadataValue) {
            PropertyMetadataValue pmv = (PropertyMetadataValue) value;
            PropertyMapKey propertyMapKey = new PropertyMapKey(pmv);
            Metadata propertyMetadata = propertyMetadatas.get(propertyMapKey);
            if (propertyMetadata == null) {
                propertyMetadata = new Metadata();
                propertyMetadatas.put(propertyMapKey, propertyMetadata);
            }
            propertyMetadata.add(pmv.getKey(), pmv.getValue(), pmv.getVisibility());
        }
    }

    protected abstract T createElement(String id);

    protected SqlGraph getGraph() {
        return graph;
    }

    protected Authorizations getAuthorizations() {
        return authorizations;
    }

    protected VertexiumSerializer getSerializer() {
        return serializer;
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

    protected EnumSet<FetchHint> getFetchHints() {
        return fetchHints;
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
