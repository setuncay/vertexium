package org.vertexium.sql.utils;

import org.vertexium.*;
import org.vertexium.mutation.PropertyDeleteMutation;
import org.vertexium.mutation.PropertySoftDeleteMutation;
import org.vertexium.property.MutablePropertyImpl;
import org.vertexium.security.ColumnVisibility;
import org.vertexium.security.VisibilityEvaluator;
import org.vertexium.security.VisibilityParseException;
import org.vertexium.sql.SqlElement;
import org.vertexium.sql.SqlGraph;
import org.vertexium.sql.SqlGraphSQL;
import org.vertexium.sql.models.ElementSignalValueBase;
import org.vertexium.sql.models.PropertyValueValue;
import org.vertexium.sql.models.SqlGraphValueBase;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public abstract class ElementResultSetIterable<T extends Element> extends SqlGraphSqlResultSetIterable<T> {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(ElementResultSetIterable.class);
    private final SqlGraphSQL sqlGraphSQL;
    private final SqlGraph graph;
    private final EnumSet<FetchHint> fetchHints;
    private final Long endTime;
    private final VertexiumSerializer serializer;
    private final Authorizations authorizations;
    private final VisibilityEvaluator visibilityEvaluator;

    public ElementResultSetIterable(
            SqlGraphSQL sqlGraphSQL,
            SqlGraph graph,
            EnumSet<FetchHint> fetchHints,
            Long endTime,
            VertexiumSerializer serializer,
            Authorizations authorizations
    ) {
        this.sqlGraphSQL = sqlGraphSQL;
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
        String id = rs.getString(SqlElement.COLUMN_ID);
        List<SqlGraphValueBase> values = new ArrayList<>();

        // TODO fetchHints
        // TODO endTime

        while (true) {
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

            if (!id.equals(rs.getString(SqlElement.COLUMN_ID))) {
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
        for (SqlGraphValueBase value : values) {
            if (value instanceof ElementSignalValueBase) {
                return (ElementSignalValueBase) value;
            }
        }
        return null;
    }

    protected List<Property> getProperties(List<SqlGraphValueBase> values) {
        Metadata propertyMetadata = new Metadata(); // TODO
        Set<Visibility> propertyHiddenVisibilities = new HashSet<>(); // TODO
        ArrayList<Property> properties = new ArrayList<>();
        for (SqlGraphValueBase value : values) {
            if (value instanceof PropertyValueValue) {
                PropertyValueValue v = (PropertyValueValue) value;
                Property property = new MutablePropertyImpl(
                        v.getPropertyKey(),
                        v.getPropertyName(),
                        v.getValue(),
                        propertyMetadata,
                        v.getPropertyTimestamp(),
                        propertyHiddenVisibilities,
                        v.getPropertyVisibility()
                );
                properties.add(property);
            }
        }
        return properties;
    }

    protected List<PropertyDeleteMutation> getPropertyDeleteMutation(List<SqlGraphValueBase> values) {
        // TODO
        return new ArrayList<>();
    }

    protected List<PropertySoftDeleteMutation> getPropertySoftDeleteMutation(List<SqlGraphValueBase> values) {
        // TODO
        return new ArrayList<>();
    }

    protected List<Visibility> getHiddenVisibilities(List<SqlGraphValueBase> values) {
        // TODO
        return new ArrayList<>();
    }
}
