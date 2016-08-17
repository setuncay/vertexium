package org.vertexium.sql.utils;

import org.vertexium.*;
import org.vertexium.mutation.PropertyDeleteMutation;
import org.vertexium.mutation.PropertySoftDeleteMutation;
import org.vertexium.security.ColumnVisibility;
import org.vertexium.security.VisibilityEvaluator;
import org.vertexium.security.VisibilityParseException;
import org.vertexium.sql.SqlElement;
import org.vertexium.sql.SqlGraph;
import org.vertexium.sql.SqlGraphSQL;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

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
        String outVertexId = null;
        String inVertexId = null;
        String label = null;
        Visibility visibility = null;
        Long timestamp = null;
        List<Property> properties = new ArrayList<>();
        List<PropertyDeleteMutation> propertyDeleteMutations = new ArrayList<>();
        List<PropertySoftDeleteMutation> propertySoftDeleteMutations = new ArrayList<>();
        List<Visibility> hiddenVisibilities = new ArrayList<>();

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

            RowType rowType = RowType.fromValue(rs.getInt(SqlElement.COLUMN_TYPE));
            switch (rowType) {
                case SIGNAL:
                    visibility = sqlGraphSQL.visibilityFromSqlString(visibilityString);
                    timestamp = rs.getLong(SqlElement.COLUMN_TIMESTAMP);
                    label = readEdgeLabelFromSignalRow(rs);
                    outVertexId = readEdgeOutVertexIdFromSignalRow(rs);
                    inVertexId = readEdgeInVertexIdFromSignalRow(rs);
                    break;
            }

            if (!rs.next()) {
                break;
            }

            if (!id.equals(rs.getString(SqlElement.COLUMN_ID))) {
                break;
            }
        }

        if (visibility == null || timestamp == null) {
            return null;
        }

        return createElement(
                id,
                outVertexId,
                inVertexId,
                label,
                visibility,
                timestamp,
                properties,
                propertyDeleteMutations,
                propertySoftDeleteMutations,
                hiddenVisibilities
        );
    }

    protected abstract String readEdgeLabelFromSignalRow(ResultSet rs) throws SQLException;

    protected abstract String readEdgeOutVertexIdFromSignalRow(ResultSet rs) throws SQLException;

    protected abstract String readEdgeInVertexIdFromSignalRow(ResultSet rs) throws SQLException;

    protected abstract T createElement(
            String id,
            String outVertexId,
            String inVertexId,
            String label,
            Visibility visibility,
            Long timestamp,
            List<Property> properties,
            List<PropertyDeleteMutation> propertyDeleteMutations,
            List<PropertySoftDeleteMutation> propertySoftDeleteMutations,
            List<Visibility> hiddenVisibilities
    );

    protected SqlGraph getGraph() {
        return graph;
    }

    protected Authorizations getAuthorizations() {
        return authorizations;
    }

    protected VertexiumSerializer getSerializer() {
        return serializer;
    }
}
