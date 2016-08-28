package org.vertexium.sql;

import org.vertexium.*;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.sql.models.PropertyValueBase;
import org.vertexium.sql.models.SqlGraphValueBase;
import org.vertexium.sql.utils.*;
import org.vertexium.util.IterableUtils;
import org.vertexium.util.StreamUtils;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

public class SqlGraphSqlImpl implements SqlGraphSql {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(SqlGraphSqlImpl.class);
    private static final String BIG_BIN_COLUMN_TYPE = "LONGBLOB";
    private static final String GET_TABLES_TABLE_NAME_COLUMN = "TABLE_NAME";
    private static final String SPV_COLUMN_ID = "id";
    private static final String SPV_COLUMN_VALUE = "value";
    private static final String SPV_COLUMN_CLASS_NAME = "class_name";
    private static final String SPV_COLUMN_LENGTH = "length";

    // MySQL's limit is 767 (http://dev.mysql.com/doc/refman/5.7/en/innodb-restrictions.html)
    private static final int ID_VARCHAR_SIZE = 767;

    // Oracle's limit is 4000 (https://docs.oracle.com/cd/B28359_01/server.111/b28320/limits001.htm)
    // MySQL's limit is 65,535 (http://dev.mysql.com/doc/refman/5.7/en/char.html)
    // H2's limit is Integer.MAX_VALUE (http://www.h2database.com/html/datatypes.html#varchar_type)
    private static final int VARCHAR_SIZE = 4000;

    private final SqlGraphConfiguration configuration;
    private final VertexiumSerializer serializer;

    public SqlGraphSqlImpl(SqlGraphConfiguration configuration, VertexiumSerializer serializer) {
        this.configuration = configuration;
        this.serializer = serializer;
    }

    @Override
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

    protected void createEdgeTable(Connection conn) {
        String edgeTableName = configuration.tableNameWithPrefix(SqlGraphConfiguration.EDGE_TABLE_NAME);
        createElementTable(conn, edgeTableName);
    }

    protected void createElementTable(Connection conn, String tableName) {
        String sql = String.format(
                "CREATE TABLE IF NOT EXISTS %s (" +
                        SqlElement.COLUMN_PK + " BIGINT PRIMARY KEY AUTO_INCREMENT" +
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

    protected void createVertexTable(Connection conn) {
        String vertexTableName = configuration.tableNameWithPrefix(SqlGraphConfiguration.VERTEX_TABLE_NAME);
        createElementTable(conn, vertexTableName);
    }

    protected void createMetadataTable(Connection conn) {
        String metadataTableName = configuration.tableNameWithPrefix(SqlGraphConfiguration.METADATA_TABLE_NAME);
        String sql = String.format(
                "CREATE TABLE IF NOT EXISTS %s (key varchar(" + ID_VARCHAR_SIZE + ") PRIMARY KEY, value %s NOT NULL)",
                metadataTableName,
                BIG_BIN_COLUMN_TYPE
        );
        runSql(conn, sql, metadataTableName);
    }

    protected void createStreamingPropertiesTable(Connection conn) {
        String tableName = configuration.tableNameWithPrefix(SqlGraphConfiguration.STREAMING_PROPERTIES_TABLE_NAME);
        String sql = String.format(
                "CREATE TABLE IF NOT EXISTS %s (" +
                        SPV_COLUMN_ID + " BIGINT PRIMARY KEY AUTO_INCREMENT" +
                        ", " + SPV_COLUMN_CLASS_NAME + " varchar(" + ID_VARCHAR_SIZE + ") NOT NULL" +
                        ", " + SPV_COLUMN_LENGTH + " INT NOT NULL" +
                        ", " + SPV_COLUMN_VALUE + " " + BIG_BIN_COLUMN_TYPE + " NOT NULL" +
                        ")",
                tableName
        );
        runSql(conn, sql, tableName);
    }

    protected static void createColumnIndexes(Connection conn, String tableName, String... columnNames) {
        for (String columnName : columnNames) {
            String sql = String.format(
                    "CREATE INDEX idx_%s_%s on %s (%s);", tableName, columnName, tableName, columnName);
            runSql(conn, sql, tableName);
        }
    }

    protected static void runSql(Connection conn, String sql, String tableName) {
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

    protected static boolean doesTableExist(Connection conn, String tableName) throws SQLException {
        ResultSet tables = conn.getMetaData().getTables(null, null, "%", null);
        while (tables.next()) {
            if (tableName.equalsIgnoreCase(tables.getString(GET_TABLES_TABLE_NAME_COLUMN))) {
                return true;
            }
        }
        return false;
    }

    @Override
    public ResultSetIterable<GraphMetadataEntry> metadataSelectAll() {
        final String sql = String.format("SELECT key, value FROM %s", configuration.tableNameWithPrefix(SqlGraphConfiguration.METADATA_TABLE_NAME));
        return new SqlGraphSqlResultSetIterable<GraphMetadataEntry>(this, new SqlPreparedStatementCreator(sql)) {
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

    @Override
    public StreamingPropertyValue selectStreamingPropertyValue(long spvRowId) {
        String sql = String.format(
                "SELECT * FROM %s WHERE %s=?",
                configuration.tableNameWithPrefix(SqlGraphConfiguration.STREAMING_PROPERTIES_TABLE_NAME),
                SPV_COLUMN_ID
        );
        try (Connection conn = getConnection()) {
            PreparedStatement stmt = conn.prepareStatement(sql);
            stmt.setLong(1, spvRowId);
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                byte[] valueBytes = rs.getBytes(SPV_COLUMN_VALUE);
                String className = rs.getString(SPV_COLUMN_CLASS_NAME);
                long length = rs.getLong(SPV_COLUMN_LENGTH);

                ByteArrayInputStream in = new ByteArrayInputStream(valueBytes);
                Class valueType;
                try {
                    valueType = Class.forName(className);
                } catch (ClassNotFoundException e) {
                    throw new VertexiumException("Could not get class: " + className, e);
                }
                return new StreamingPropertyValue(in, valueType, length);
            }
        } catch (SQLException ex) {
            throw new VertexiumException("Could not read StreamingPropertyValue", ex);
        }
    }

    @Override
    public void deletePropertyRows(
            Connection conn,
            ElementType elementType,
            String elementId,
            String propertyKey,
            String propertyName,
            Visibility propertyVisibility
    ) {
        try {
            String tableName = getTableNameFromElementType(elementType);
            String sql = String.format("SELECT * FROM %s WHERE %s=?", tableName, SqlElement.COLUMN_ID);
            List<Long> toDelete = new ArrayList<>();
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, elementId);
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        long pk = rs.getLong(SqlElement.COLUMN_PK);
                        byte[] valueBytes = rs.getBytes(SqlElement.COLUMN_VALUE);
                        Object valueObject = serializer.bytesToObject(valueBytes);
                        if (!(valueObject instanceof PropertyValueBase)) {
                            continue;
                        }
                        PropertyValueBase value = (PropertyValueBase) valueObject;
                        if (!propertyKey.equals(value.getPropertyKey())) {
                            continue;
                        }
                        if (!propertyName.equals(value.getPropertyName())) {
                            continue;
                        }
                        if (!propertyVisibility.equals(value.getPropertyVisibility())) {
                            continue;
                        }
                        toDelete.add(pk);
                    }
                }
            }

            for (Long pk : toDelete) {
                sql = String.format("DELETE FROM %s where %s=?", tableName, SqlElement.COLUMN_PK);
                try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                    stmt.setLong(1, pk);
                    stmt.executeUpdate();
                }
            }
        } catch (SQLException ex) {
            throw new VertexiumException("Could not delete property rows", ex);
        }
    }

    @Override
    public Connection getConnection() throws SQLException {
        return configuration.getDataSource().getConnection();
    }

    @Override
    public void metadataSetMetadata(String key, Object value) {
        byte[] valueBytes = GraphMetadataEntry.serializeValue(value);
        try (Connection conn = SqlGraphSqlImpl.this.getConnection()) {
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

    @Override
    public long insertStreamingPropertyValue(Connection conn, StreamingPropertyValue spv) {
        String tableName = configuration.tableNameWithPrefix(SqlGraphConfiguration.STREAMING_PROPERTIES_TABLE_NAME);
        String sql = String.format(
                "INSERT INTO %s (%s, %s, %s) VALUES (?, ?, ?)",
                tableName,
                SPV_COLUMN_CLASS_NAME,
                SPV_COLUMN_LENGTH,
                SPV_COLUMN_VALUE
        );
        try {
            byte[] bytes = StreamUtils.toBytes(spv.getInputStream());
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, spv.getValueType().getName());
                stmt.setLong(2, spv.getLength());
                stmt.setBytes(3, bytes);
                stmt.executeUpdate();
                try (ResultSet generatedKeys = stmt.getGeneratedKeys()) {
                    if (generatedKeys.next()) {
                        return generatedKeys.getLong(1);
                    } else {
                        throw new VertexiumException("no ID obtained");
                    }
                }
            }
        } catch (Exception ex) {
            throw new VertexiumException("Could not insert row: " + tableName + ": " + sql, ex);
        }
    }

    @Override
    public void insertElementRow(
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

    protected String getTableNameFromElementType(ElementType elementType) {
        switch (elementType) {
            case EDGE:
                return configuration.tableNameWithPrefix(SqlGraphConfiguration.EDGE_TABLE_NAME);
            case VERTEX:
                return configuration.tableNameWithPrefix(SqlGraphConfiguration.VERTEX_TABLE_NAME);
            default:
                throw new VertexiumException("Invalid element type: " + elementType);
        }
    }

    protected byte[] valueToBytes(Object value) {
        return serializer.objectToBytes(value);
    }

    protected String visibilityToSqlString(Visibility visibility) {
        return visibility.getVisibilityString();
    }

    @Override
    public Iterable<Vertex> selectAllVertices(SqlGraph graph, EnumSet<FetchHint> fetchHints, Long endTime, Authorizations authorizations) {
        String whereClause;
        if (endTime == null) {
            whereClause = "";
        } else {
            whereClause = String.format("WHERE %s <= %d", SqlEdge.COLUMN_TIMESTAMP, endTime);
        }

        final String sql = String.format(
                "SELECT * FROM %s %s ORDER BY %s, %s",
                configuration.tableNameWithPrefix(SqlGraphConfiguration.VERTEX_TABLE_NAME),
                whereClause,
                SqlVertex.COLUMN_ID,
                SqlVertex.COLUMN_TIMESTAMP
        );

        return new VertexResultSetIterable(this, graph, fetchHints, endTime, serializer, authorizations, new SqlPreparedStatementCreator(sql));
    }

    public <T extends Element> T selectElement(
            SqlGraph graph,
            final String elementId,
            ElementType elementType,
            EnumSet<FetchHint> fetchHints,
            Long endTime,
            Authorizations authorizations
    ) {
        String timestampClause;
        if (endTime == null) {
            timestampClause = "";
        } else {
            timestampClause = String.format("AND %s <= %d", SqlEdge.COLUMN_TIMESTAMP, endTime);
        }

        final String sql = String.format(
                "SELECT * FROM %s WHERE %s=? %s ORDER BY %s, %s",
                getTableNameFromElementType(elementType),
                SqlElement.COLUMN_ID,
                timestampClause,
                SqlElement.COLUMN_ID,
                SqlElement.COLUMN_TIMESTAMP
        );

        PreparedStatementCreator psc = new SqlPreparedStatementCreator(sql) {
            @Override
            public PreparedStatement getStatement(Connection conn) throws SQLException {
                PreparedStatement stmt = super.getStatement(conn);
                stmt.setString(1, elementId);
                return stmt;
            }
        };
        try (ElementResultSetIterable<T> it = ElementResultSetIterable.create(this, graph, elementType, fetchHints, endTime, serializer, authorizations, psc)) {
            return IterableUtils.singleOrDefault(it, null);
        } catch (IOException e) {
            throw new VertexiumException("Could not close iterator", e);
        }
    }

    @Override
    public Vertex selectVertex(SqlGraph graph, String vertexId, EnumSet<FetchHint> fetchHints, Long endTime, Authorizations authorizations) {
        return selectElement(graph, vertexId, ElementType.VERTEX, fetchHints, endTime, authorizations);
    }

    @Override
    public Edge selectEdge(SqlGraph graph, final String edgeId, EnumSet<FetchHint> fetchHints, Long endTime, Authorizations authorizations) {
        return selectElement(graph, edgeId, ElementType.EDGE, fetchHints, endTime, authorizations);
    }

    @Override
    public Iterable<Edge> selectAllEdges(SqlGraph graph, EnumSet<FetchHint> fetchHints, Long endTime, Authorizations authorizations) {
        String whereClause;
        if (endTime == null) {
            whereClause = "";
        } else {
            whereClause = String.format("WHERE %s <= %d", SqlEdge.COLUMN_TIMESTAMP, endTime);
        }

        final String sql = String.format(
                "SELECT * FROM %s %s ORDER BY %s, %s",
                configuration.tableNameWithPrefix(SqlGraphConfiguration.EDGE_TABLE_NAME),
                whereClause,
                SqlEdge.COLUMN_ID,
                SqlEdge.COLUMN_TIMESTAMP
        );
        return new EdgeResultSetIterable(this, graph, fetchHints, endTime, serializer, authorizations, new SqlPreparedStatementCreator(sql));
    }

    @Override
    public void truncate() {
        try (Connection conn = getConnection()) {
            truncateTable(conn, configuration.tableNameWithPrefix(SqlGraphConfiguration.VERTEX_TABLE_NAME));
            truncateTable(conn, configuration.tableNameWithPrefix(SqlGraphConfiguration.EDGE_TABLE_NAME));
            truncateTable(conn, configuration.tableNameWithPrefix(SqlGraphConfiguration.STREAMING_PROPERTIES_TABLE_NAME));
            truncateTable(conn, configuration.tableNameWithPrefix(SqlGraphConfiguration.METADATA_TABLE_NAME));
        } catch (SQLException e) {
            throw new VertexiumException("Could not delete from tables", e);
        }
    }

    protected void truncateTable(Connection conn, String tableName) {
        try {
            try (PreparedStatement stmt = conn.prepareStatement(String.format("DELETE FROM %s", tableName))) {
                stmt.executeUpdate();
            }
        } catch (SQLException e) {
            throw new VertexiumException("Could not delete from: " + tableName, e);
        }
    }
}
