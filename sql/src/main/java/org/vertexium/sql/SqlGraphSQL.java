package org.vertexium.sql;

import org.vertexium.*;
import org.vertexium.mutation.PropertyDeleteMutation;
import org.vertexium.mutation.PropertySoftDeleteMutation;
import org.vertexium.sql.utils.ResultSetIterable;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

public class SqlGraphSQL {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(SqlGraphSQL.class);
    private static final String BIG_BIN_COLUMN_TYPE = "LONGBLOB";
    private static final String GET_TABLES_TABLE_NAME_COLUMN = "TABLE_NAME";
    private static final String VERTEX_TABLE_SELECT_COLUMNS = "key, visibility, type, timestamp";

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

    public void createTables(Connection conn) {
        createVertexTable(conn);
        createTable(
                conn,
                configuration.tableNameWithPrefix(SqlGraphConfiguration.EDGE_TABLE_NAME),
                BIG_BIN_COLUMN_TYPE,
                "in_vertex_id varchar(" + ID_VARCHAR_SIZE + "), out_vertex_id varchar(" + ID_VARCHAR_SIZE + ")"
        );
        createColumnIndexes(
                conn,
                configuration.tableNameWithPrefix(SqlGraphConfiguration.EDGE_TABLE_NAME),
                "in_vertex_id", "out_vertex_id"
        );

        createMetadataTable(conn);

        createStreamingPropertiesTable(
                conn,
                configuration.tableNameWithPrefix(SqlGraphConfiguration.STREAMING_PROPERTIES_TABLE_NAME)
        );
    }

    private void createVertexTable(Connection conn) {
        String metadataTableName = configuration.tableNameWithPrefix(SqlGraphConfiguration.VERTEX_TABLE_NAME);
        String sql = String.format(
                "CREATE TABLE IF NOT EXISTS %s (" +
                        "id BIGINT PRIMARY KEY AUTO_INCREMENT" +
                        ", key VARCHAR(" + VARCHAR_SIZE + ")" +
                        ", visibility VARCHAR(" + VARCHAR_SIZE + ")" +
                        ", type INT" +
                        ", timestamp BIGINT" +
                        ")",
                metadataTableName
        );
        runSql(conn, sql, metadataTableName);
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

    private static void createTable(Connection connection, String tableName, String valueColumnType) {
        createTable(connection, tableName, valueColumnType, "");
    }

    private static void createTable(Connection connection, String tableName, String valueColumnType, String additionalColumns) {
        if (!additionalColumns.isEmpty()) {
            additionalColumns = ", " + additionalColumns;
        }
        String sql = String.format(
                "CREATE TABLE IF NOT EXISTS %s (%s varchar(" + ID_VARCHAR_SIZE + ") primary key, %s %s not null %s)",
                tableName,
                SqlGraphConfiguration.KEY_COLUMN_NAME,
                SqlGraphConfiguration.VALUE_COLUMN_NAME,
                valueColumnType,
                additionalColumns
        );
        runSql(connection, sql, tableName);
    }

    private static void createStreamingPropertiesTable(Connection conn, String tableName) {
        // TODO
//        String sql = String.format(
//                "CREATE TABLE IF NOT EXISTS %s (%s varchar(" + ID_VARCHAR_SIZE + ") primary key, %s %s not null, %s varchar(" + VARCHAR_SIZE + ") not null, %s bigint not null)",
//                tableName,
//                SqlStreamingPropertyTable.KEY_COLUMN_NAME,
//                SqlStreamingPropertyTable.VALUE_COLUMN_NAME,
//                BIG_BIN_COLUMN_TYPE,
//                SqlStreamingPropertyTable.VALUE_TYPE_COLUMN_NAME,
//                SqlStreamingPropertyTable.VALUE_LENGTH_COLUMN_NAME);
//        runSql(dataSource, sql, tableName);
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

    public void metadataSetMetadata(String key, Object value) {
        byte[] valueBytes = GraphMetadataEntry.serializeValue(value);
        try (Connection conn = configuration.getDataSource().getConnection()) {
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

    public void verticesSaveVertexBuilder(SqlGraph sqlGraph, SqlVertexBuilder vertexBuilder, long timestamp) {
        String vertexRowKey = vertexBuilder.getVertexId();

        try (Connection conn = configuration.getDataSource().getConnection()) {
            insertVertexSignalRow(conn, vertexRowKey, timestamp, vertexBuilder.getVisibility());

            // TODO
//        for (PropertyDeleteMutation propertyDeleteMutation : vertexBuilder.getPropertyDeletes()) {
//            addPropertyDeleteToMutation(m, propertyDeleteMutation);
//        }
//        for (PropertySoftDeleteMutation propertySoftDeleteMutation : vertexBuilder.getPropertySoftDeletes()) {
//            addPropertySoftDeleteToMutation(m, propertySoftDeleteMutation);
//        }
//        for (Property property : vertexBuilder.getProperties()) {
//            addPropertyToMutation(graph, m, vertexRowKey, property);
//        }
        } catch (SQLException ex) {
            throw new VertexiumException("Could not save vertex builder: " + vertexRowKey, ex);
        }
    }

    private void insertVertexSignalRow(Connection conn, String vertexRowKey, long timestamp, Visibility visibility) throws SQLException {
        String sql = String.format("INSERT INTO %s (key, visibility, type, timestamp) VALUES (?, ?, ?, ?)", configuration.tableNameWithPrefix(SqlGraphConfiguration.VERTEX_TABLE_NAME));
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, vertexRowKey);
            stmt.setString(2, visibilityToSqlString(visibility));
            stmt.setInt(3, RowType.SIGNAL.getValue());
            stmt.setLong(4, timestamp);
            stmt.executeUpdate();
        }
    }

    private String visibilityToSqlString(Visibility visibility) {
        return visibility.getVisibilityString();
    }

    private Visibility visibilityFromSqlString(String visibilityString) {
        return new Visibility(visibilityString);
    }

    public Iterable<Vertex> verticesSelectAll(SqlGraph graph, EnumSet<FetchHint> fetchHints, Long endTime, Authorizations authorizations) {
        final String sql = String.format("SELECT %s FROM %s", VERTEX_TABLE_SELECT_COLUMNS, configuration.tableNameWithPrefix(SqlGraphConfiguration.VERTEX_TABLE_NAME));
        return new VertexResultSetIterable(graph, fetchHints, endTime, authorizations) {
            @Override
            protected PreparedStatement getStatement(Connection conn) throws SQLException {
                return conn.prepareStatement(sql);
            }
        };
    }

    private enum RowType {
        SIGNAL(1);

        private final int value;

        RowType(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

        public static RowType fromValue(int type) {
            switch (type) {
                case 1:
                    return SIGNAL;
            }
            throw new VertexiumException("Unhandled row type: " + type);
        }
    }

    private abstract class SqlGraphSqlResultSetIterable<T> extends ResultSetIterable<T> {
        @Override
        protected Connection getConnection() throws SQLException {
            return configuration.getDataSource().getConnection();
        }
    }

    private abstract class VertexResultSetIterable extends SqlGraphSqlResultSetIterable<Vertex> {
        private final SqlGraph graph;
        private final EnumSet<FetchHint> fetchHints;
        private final Long endTime;
        private final Authorizations authorizations;

        public VertexResultSetIterable(SqlGraph graph, EnumSet<FetchHint> fetchHints, Long endTime, Authorizations authorizations) {
            this.graph = graph;
            this.fetchHints = fetchHints;
            this.endTime = endTime;
            this.authorizations = authorizations;
        }

        @Override
        protected Vertex readFromResultSet(ResultSet rs) throws SQLException {
            if (rs.isBeforeFirst()) {
                if (!rs.next()) {
                    return null;
                }
            }

            Vertex v;
            do {
                v = readVertex(rs);
            } while (v == null);
            return v;
        }

        private Vertex readVertex(ResultSet rs) throws SQLException {
            String key = rs.getString("key");
            Visibility visibility = null;
            Long timestamp = null;
            List<Property> properties = new ArrayList<>();
            List<PropertyDeleteMutation> propertyDeleteMutations = new ArrayList<>();
            List<PropertySoftDeleteMutation> propertySoftDeleteMutations = new ArrayList<>();
            List<Visibility> hiddenVisibilities = new ArrayList<>();

            // TODO fetchHints
            // TODO endTime

            while (true) {
                RowType rowType = RowType.fromValue(rs.getInt("type"));
                switch (rowType) {
                    case SIGNAL:
                        visibility = visibilityFromSqlString(rs.getString("visibility"));
                        timestamp = rs.getLong("timestamp");
                        break;
                }

                if (!rs.next()) {
                    break;
                }

                if (!key.equals(rs.getString("key"))) {
                    break;
                }
            }

            if (visibility == null || timestamp == null) {
                return null;
            }

            return new SqlVertex(
                    graph,
                    key,
                    visibility,
                    properties,
                    propertyDeleteMutations,
                    propertySoftDeleteMutations,
                    hiddenVisibilities,
                    timestamp,
                    authorizations
            );
        }
    }
}
