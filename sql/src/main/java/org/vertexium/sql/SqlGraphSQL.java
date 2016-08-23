package org.vertexium.sql;

import org.vertexium.*;
import org.vertexium.mutation.ExistingElementMutationImpl;
import org.vertexium.mutation.PropertyDeleteMutation;
import org.vertexium.mutation.PropertySoftDeleteMutation;
import org.vertexium.search.IndexHint;
import org.vertexium.sql.models.*;
import org.vertexium.sql.utils.*;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.sql.*;
import java.util.EnumSet;

public class SqlGraphSQL {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(SqlGraphSQL.class);
    private static final String BIG_BIN_COLUMN_TYPE = "LONGBLOB";
    private static final String GET_TABLES_TABLE_NAME_COLUMN = "TABLE_NAME";

    // MySQL's limit is 767 (http://dev.mysql.com/doc/refman/5.7/en/innodb-restrictions.html)
    private static final int ID_VARCHAR_SIZE = 767;

    // Oracle's limit is 4000 (https://docs.oracle.com/cd/B28359_01/server.111/b28320/limits001.htm)
    // MySQL's limit is 65,535 (http://dev.mysql.com/doc/refman/5.7/en/char.html)
    // H2's limit is Integer.MAX_VALUE (http://www.h2database.com/html/datatypes.html#varchar_type)
    private static final int VARCHAR_SIZE = 4000;

    private final SqlGraphConfiguration configuration;
    private final VertexiumSerializer serializer;

    public SqlGraphSQL(SqlGraphConfiguration configuration, VertexiumSerializer serializer) {
        this.configuration = configuration;
        this.serializer = serializer;
    }

    public void createTables() {
        try (Connection conn = getConnection()) {
            createVertexTable(conn);
            createEdgeTable(conn);
            createMetadataTable(conn);

            createStreamingPropertiesTable(conn);
        } catch (SQLException e) {
            throw new VertexiumException("Could not create tables", e);
        }
    }

    private void createEdgeTable(Connection conn) {
        String edgeTableName = configuration.tableNameWithPrefix(SqlGraphConfiguration.EDGE_TABLE_NAME);
        createElementTable(conn, edgeTableName);
    }

    private void createElementTable(Connection conn, String tableName) {
        String sql = String.format(
                "CREATE TABLE IF NOT EXISTS %s (" +
                        "id BIGINT PRIMARY KEY AUTO_INCREMENT" +
                        ", " + SqlElement.COLUMN_ID + " VARCHAR(" + VARCHAR_SIZE + ") NOT NULL" +
                        ", " + SqlElement.COLUMN_VISIBILITY + " VARCHAR(" + VARCHAR_SIZE + ") NOT NULL" +
                        ", " + SqlElement.COLUMN_TYPE + " INT NOT NULL" +
                        ", " + SqlElement.COLUMN_TIMESTAMP + " BIGINT NOT NULL" +
                        ", " + SqlElement.COLUMN_VALUE + " " + BIG_BIN_COLUMN_TYPE + " NOT NULL" +
                        ")",
                tableName
        );
        runSql(conn, sql, tableName);

        createColumnIndexes(conn, tableName, SqlElement.COLUMN_ID);
    }

    private void createVertexTable(Connection conn) {
        String vertexTableName = configuration.tableNameWithPrefix(SqlGraphConfiguration.VERTEX_TABLE_NAME);
        createElementTable(conn, vertexTableName);
    }

    private void createMetadataTable(Connection conn) {
        String metadataTableName = configuration.tableNameWithPrefix(SqlGraphConfiguration.METADATA_TABLE_NAME);
        String sql = String.format(
                "CREATE TABLE IF NOT EXISTS %s (key varchar(" + ID_VARCHAR_SIZE + ") PRIMARY KEY, value %s NOT NULL)",
                metadataTableName,
                BIG_BIN_COLUMN_TYPE
        );
        runSql(conn, sql, metadataTableName);
    }

    private void createStreamingPropertiesTable(Connection conn) {
        String tableName = configuration.tableNameWithPrefix(SqlGraphConfiguration.STREAMING_PROPERTIES_TABLE_NAME);
        String sql = String.format(
                "CREATE TABLE IF NOT EXISTS %s (" +
                        "id BIGINT PRIMARY KEY AUTO_INCREMENT" +
                        ", " + SqlElement.COLUMN_VALUE + " " + BIG_BIN_COLUMN_TYPE + " NOT NULL" +
                        ")",
                tableName
        );
        runSql(conn, sql, tableName);
    }

    private static void createColumnIndexes(Connection conn, String tableName, String... columnNames) {
        for (String columnName : columnNames) {
            String sql = String.format(
                    "CREATE INDEX idx_%s_%s on %s (%s);", tableName, columnName, tableName, columnName);
            runSql(conn, sql, tableName);
        }
    }

    private static void runSql(Connection conn, String sql, String tableName) {
        try {
            if (!doesTableExist(conn, tableName)) {
                LOGGER.info("creating table %s (sql: %s)", tableName, sql);
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute(sql);
                }
            }
        } catch (SQLException ex) {
            throw new VertexiumException("Could not create SQL table: " + tableName + " (sql: " + sql + ")", ex);
        }
    }

    private static boolean doesTableExist(Connection conn, String tableName) throws SQLException {
        ResultSet tables = conn.getMetaData().getTables(null, null, "%", null);
        while (tables.next()) {
            if (tableName.equalsIgnoreCase(tables.getString(GET_TABLES_TABLE_NAME_COLUMN))) {
                return true;
            }
        }
        return false;
    }

    public ResultSetIterable<GraphMetadataEntry> metadataSelectAll() {
        final String sql = String.format("SELECT key, value FROM %s", configuration.tableNameWithPrefix(SqlGraphConfiguration.METADATA_TABLE_NAME));
        return new SqlGraphSqlResultSetIterable<GraphMetadataEntry>() {
            @Override
            protected Connection getConnection() throws SQLException {
                return SqlGraphSQL.this.getConnection();
            }

            @Override
            protected PreparedStatement getStatement(Connection conn) throws SQLException {
                return conn.prepareStatement(sql);
            }

            @Override
            protected GraphMetadataEntry readFromResultSet(ResultSet rs) throws SQLException {
                if (!rs.next()) {
                    return null;
                }

                String key = rs.getString("key");
                byte[] value = rs.getBytes("value");
                return new GraphMetadataEntry(key, value);
            }
        };
    }

    private Connection getConnection() throws SQLException {
        return configuration.getDataSource().getConnection();
    }

    public void metadataSetMetadata(String key, Object value) {
        byte[] valueBytes = GraphMetadataEntry.serializeValue(value);
        try (Connection conn = SqlGraphSQL.this.getConnection()) {
            String sql = String.format("INSERT INTO %s (key, value) VALUES (?, ?)", configuration.tableNameWithPrefix(SqlGraphConfiguration.METADATA_TABLE_NAME));
            try (PreparedStatement insertStatement = conn.prepareStatement(sql)) {
                insertStatement.setString(1, key);
                insertStatement.setBytes(2, valueBytes);
                try {
                    insertStatement.executeUpdate();
                } catch (SQLException ex) {
                    sql = String.format("UPDATE %s SET value=? WHERE key=?", configuration.tableNameWithPrefix(SqlGraphConfiguration.METADATA_TABLE_NAME));
                    try (PreparedStatement updateStatement = conn.prepareStatement(sql)) {
                        updateStatement.setString(2, key);
                        updateStatement.setBytes(1, valueBytes);
                        updateStatement.executeUpdate();
                    }
                }
            }
        } catch (SQLException ex) {
            throw new VertexiumException("Could not set metadata: " + key, ex);
        }
    }

    public void saveVertexBuilder(SqlGraph sqlGraph, SqlVertexBuilder vertexBuilder, long timestamp) {
        String vertexRowKey = vertexBuilder.getVertexId();

        try (Connection conn = getConnection()) {
            insertVertexSignalRow(conn, vertexRowKey, timestamp, vertexBuilder.getVisibility());

            // TODO
//        for (PropertyDeleteMutation propertyDeleteMutation : vertexBuilder.getPropertyDeletes()) {
//            addPropertyDeleteToMutation(m, propertyDeleteMutation);
//        }
//        for (PropertySoftDeleteMutation propertySoftDeleteMutation : vertexBuilder.getPropertySoftDeletes()) {
//            addPropertySoftDeleteToMutation(m, propertySoftDeleteMutation);
//        }
            for (Property property : vertexBuilder.getProperties()) {
                insertElementPropertyRow(sqlGraph, conn, ElementType.VERTEX, vertexRowKey, property);
            }
        } catch (SQLException ex) {
            throw new VertexiumException("Could not save vertex builder: " + vertexRowKey, ex);
        }
    }

    private void insertVertexSignalRow(
            Connection conn,
            String vertexId,
            long timestamp,
            Visibility visibility
    ) throws SQLException {
        VertexSignalValue value = new VertexSignalValue(timestamp, visibility);
        insertElementRow(conn, ElementType.VERTEX, vertexId, RowType.SIGNAL, timestamp, visibility, value);
    }

    private void insertEdgeSignalRow(
            Connection conn,
            String edgeId,
            String edgeLabel,
            String outVertexId,
            String inVertexId,
            long timestamp,
            Visibility visibility
    ) {
        EdgeSignalValue value = new EdgeSignalValue(timestamp, edgeLabel, outVertexId, inVertexId, visibility);
        insertElementRow(conn, ElementType.EDGE, edgeId, RowType.SIGNAL, timestamp, visibility, value);
    }

    private void insertElementPropertyRow(
            SqlGraph graph,
            Connection conn,
            ElementType elementType,
            String elementId,
            Property property
    ) {
        graph.ensurePropertyDefined(property.getName(), property.getValue());

        PropertyValueValue value = new PropertyValueValue(
                property.getKey(),
                property.getName(),
                property.getTimestamp(),
                property.getValue(),
                property.getVisibility()
        );
        insertElementRow(
                conn,
                elementType,
                elementId,
                RowType.PROPERTY,
                property.getTimestamp(),
                property.getVisibility(),
                value
        );
    }

    private void insertElementRow(
            Connection conn,
            ElementType elementType,
            String elementId,
            RowType rowType,
            long timestamp,
            Visibility visibility,
            SqlGraphValueBase value
    ) {
        String tableName = getTableNameFromElementType(elementType);
        String sql = String.format(
                "INSERT INTO %s (%s, %s, %s, %s, %s) VALUES (?, ?, ?, ?, ?)",
                tableName,
                SqlVertex.COLUMN_ID,
                SqlVertex.COLUMN_VISIBILITY,
                SqlVertex.COLUMN_TYPE,
                SqlVertex.COLUMN_TIMESTAMP,
                SqlVertex.COLUMN_VALUE
        );
        try {
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, elementId);
                stmt.setString(2, visibilityToSqlString(visibility));
                stmt.setInt(3, rowType.getValue());
                stmt.setLong(4, timestamp);
                stmt.setBytes(5, valueToBytes(value));
                stmt.executeUpdate();
            }
        } catch (Exception ex) {
            throw new VertexiumException("Could not insert row: " + tableName + ": " + sql, ex);
        }
    }

    private String getTableNameFromElementType(ElementType elementType) {
        switch (elementType) {
            case EDGE:
                return configuration.tableNameWithPrefix(SqlGraphConfiguration.EDGE_TABLE_NAME);
            case VERTEX:
                return configuration.tableNameWithPrefix(SqlGraphConfiguration.VERTEX_TABLE_NAME);
            default:
                throw new VertexiumException("Invalid element type: " + elementType);
        }
    }

    private byte[] valueToBytes(Object value) {
        return serializer.objectToBytes(value);
    }

    private String visibilityToSqlString(Visibility visibility) {
        return visibility.getVisibilityString();
    }

    public Visibility visibilityFromSqlString(String visibilityString) {
        return new Visibility(visibilityString);
    }

    public Iterable<Vertex> selectAllVertices(SqlGraph graph, EnumSet<FetchHint> fetchHints, Long endTime, Authorizations authorizations) {
        final String sql = String.format(
                "SELECT * FROM %s ORDER BY %s",
                configuration.tableNameWithPrefix(SqlGraphConfiguration.VERTEX_TABLE_NAME),
                SqlVertex.COLUMN_ID
        );
        return new VertexResultSetIterable(this, graph, fetchHints, endTime, serializer, authorizations) {
            @Override
            protected Connection getConnection() throws SQLException {
                return SqlGraphSQL.this.getConnection();
            }

            @Override
            protected PreparedStatement getStatement(Connection conn) throws SQLException {
                return conn.prepareStatement(sql);
            }
        };
    }

    public void saveEdgeBuilder(SqlGraph sqlGraph, EdgeBuilder edgeBuilder, long timestamp) {
        Visibility visibility = edgeBuilder.getVisibility();

        try (Connection conn = SqlGraphSQL.this.getConnection()) {
            saveToEdgeTable(sqlGraph, conn, edgeBuilder, visibility, timestamp);

            String edgeLabel = edgeBuilder.getNewEdgeLabel() != null ? edgeBuilder.getNewEdgeLabel() : edgeBuilder.getLabel();

            saveEdgeInfoOnVertex(
                    conn,
                    edgeBuilder.getEdgeId(),
                    edgeBuilder.getOutVertexId(),
                    edgeBuilder.getInVertexId(),
                    edgeLabel,
                    timestamp,
                    edgeBuilder.getVisibility()
            );
        } catch (SQLException ex) {
            throw new VertexiumException("Could not save edge builder: " + edgeBuilder.getEdgeId(), ex);
        }
    }

    private void saveEdgeInfoOnVertex(
            Connection conn,
            String edgeId,
            String outVertexId,
            String inVertexId,
            String edgeLabel,
            long timestamp,
            Visibility edgeVisibility
    ) {
        EdgeInfoValue edgeInfoValue = new EdgeInfoValue(Direction.OUT, edgeId, edgeLabel, inVertexId, edgeVisibility);
        insertElementRow(conn, ElementType.VERTEX, outVertexId, RowType.OUT_EDGE_INFO, timestamp, edgeVisibility, edgeInfoValue);

        edgeInfoValue = new EdgeInfoValue(Direction.IN, edgeId, edgeLabel, outVertexId, edgeVisibility);
        insertElementRow(conn, ElementType.VERTEX, inVertexId, RowType.IN_EDGE_INFO, timestamp, edgeVisibility, edgeInfoValue);
    }

    private void saveToEdgeTable(
            SqlGraph sqlGraph,
            Connection conn,
            EdgeBuilder edgeBuilder,
            Visibility visibility,
            long timestamp
    ) throws SQLException {
        String edgeId = edgeBuilder.getEdgeId();
        String edgeLabel = edgeBuilder.getLabel();
        if (edgeBuilder.getNewEdgeLabel() != null) {
            edgeLabel = edgeBuilder.getNewEdgeLabel();
            // TODO delete old edge label
            // m.putDelete(AccumuloEdge.CF_SIGNAL, new Text(edgeBuilder.getLabel()), edgeColumnVisibility, currentTimeMillis());
        }

        String outVertexId = edgeBuilder.getOutVertexId();
        String inVertexId = edgeBuilder.getInVertexId();
        insertEdgeSignalRow(conn, edgeId, edgeLabel, outVertexId, inVertexId, timestamp, visibility);
        // TODO m.put(AccumuloEdge.CF_OUT_VERTEX, new Text(edgeBuilder.getOutVertexId()), edgeColumnVisibility, timestamp, ElementMutationBuilder.EMPTY_VALUE);
        // TODO m.put(AccumuloEdge.CF_IN_VERTEX, new Text(edgeBuilder.getInVertexId()), edgeColumnVisibility, timestamp, ElementMutationBuilder.EMPTY_VALUE);

        // TODO save properties
//        for (PropertyDeleteMutation propertyDeleteMutation : edgeBuilder.getPropertyDeletes()) {
//            addPropertyDeleteToMutation(m, propertyDeleteMutation);
//        }
//        for (PropertySoftDeleteMutation propertySoftDeleteMutation : edgeBuilder.getPropertySoftDeletes()) {
//            addPropertySoftDeleteToMutation(m, propertySoftDeleteMutation);
//        }
        for (Property property : edgeBuilder.getProperties()) {
            insertElementPropertyRow(sqlGraph, conn, ElementType.EDGE, edgeId, property);
        }
    }

    public Iterable<Edge> selectAllEdges(SqlGraph graph, EnumSet<FetchHint> fetchHints, Long endTime, Authorizations authorizations) {
        final String sql = String.format(
                "SELECT * FROM %s ORDER BY %s",
                configuration.tableNameWithPrefix(SqlGraphConfiguration.EDGE_TABLE_NAME),
                SqlEdge.COLUMN_ID
        );
        return new EdgeResultSetIterable(this, graph, fetchHints, endTime, serializer, authorizations) {
            @Override
            protected Connection getConnection() throws SQLException {
                return SqlGraphSQL.this.getConnection();
            }

            @Override
            protected PreparedStatement getStatement(Connection conn) throws SQLException {
                return conn.prepareStatement(sql);
            }
        };
    }

    public void saveExistingElementMutation(
            SqlGraph graph,
            ExistingElementMutationImpl<Vertex> mutation,
            Authorizations authorizations
    ) {
        try (Connection conn = getConnection()) {
            // Order matters a lot here

            // metadata must be altered first because the lookup of a property can include visibility which will be altered by alterElementPropertyVisibilities
            // TODO getGraph().alterPropertyMetadatas((AccumuloElement) mutation.getElement(), mutation.getSetPropertyMetadatas());

            // altering properties comes next because alterElementVisibility may alter the vertex and we won't find it
            // TODO
//        getGraph().alterElementPropertyVisibilities(
//                (AccumuloElement) mutation.getElement(),
//                mutation.getAlterPropertyVisibilities(),
//                authorizations
//        );

            Iterable<PropertyDeleteMutation> propertyDeletes = mutation.getPropertyDeletes();
            Iterable<PropertySoftDeleteMutation> propertySoftDeletes = mutation.getPropertySoftDeletes();
            Iterable<Property> properties = mutation.getProperties();

            // TODO
//        overridePropertyTimestamps(properties);

            ((SqlElement) mutation.getElement()).updatePropertiesInternal(properties, propertyDeletes, propertySoftDeletes);
            saveProperties(
                    conn,
                    graph,
                    (SqlElement) mutation.getElement(),
                    properties,
                    propertyDeletes,
                    propertySoftDeletes,
                    mutation.getIndexHint(),
                    authorizations
            );

            // TODO
//        if (mutation.getNewElementVisibility() != null) {
//            getGraph().alterElementVisibility((AccumuloElement) mutation.getElement(), mutation.getNewElementVisibility(), authorizations);
//        }
//
//        if (mutation instanceof EdgeMutation) {
//            EdgeMutation edgeMutation = (EdgeMutation) mutation;
//
//            String newEdgeLabel = edgeMutation.getNewEdgeLabel();
//            if (newEdgeLabel != null) {
//                getGraph().alterEdgeLabel((AccumuloEdge) mutation.getElement(), newEdgeLabel);
//            }
//        }
        } catch (SQLException e) {
            throw new VertexiumException("Could not save existing element mutation", e);
        }
    }

    void saveProperties(
            Connection conn,
            SqlGraph graph,
            SqlElement element,
            Iterable<Property> properties,
            Iterable<PropertyDeleteMutation> propertyDeletes,
            Iterable<PropertySoftDeleteMutation> propertySoftDeletes,
            IndexHint indexHint,
            Authorizations authorizations
    ) throws SQLException {
        String elementRowKey = element.getId();
        // TODO
//        for (PropertyDeleteMutation propertyDelete : propertyDeletes) {
//            elementMutationBuilder.addPropertyDeleteToMutation(m, propertyDelete);
//        }
//        for (PropertySoftDeleteMutation propertySoftDelete : propertySoftDeletes) {
//            elementMutationBuilder.addPropertySoftDeleteToMutation(m, propertySoftDelete);
//        }
        for (Property property : properties) {
            ElementType elementType = ElementType.getTypeFromElement(element);
            insertElementPropertyRow(graph, conn, elementType, elementRowKey, property);
        }

        graph.saveProperties(
                element,
                properties,
                propertyDeletes,
                propertySoftDeletes,
                indexHint,
                authorizations
        );
    }
}
