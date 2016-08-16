package org.vertexium.sql;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.vertexium.GraphConfiguration;
import org.vertexium.VertexiumSerializer;

import javax.sql.DataSource;
import java.util.Map;
import java.util.Properties;

public class SqlGraphConfiguration extends GraphConfiguration {
    protected static final String KEY_COLUMN_NAME = "id";
    protected static final String VALUE_COLUMN_NAME = "object";
    protected static final String VERTEX_TABLE_NAME = "vertex";
    protected static final String EDGE_TABLE_NAME = "edge";
    protected static final String METADATA_TABLE_NAME = "metadata";
    protected static final String STREAMING_PROPERTIES_TABLE_NAME = "streaming_properties";
    protected static final String IN_VERTEX_ID_COLUMN = "in_vertex_id";
    protected static final String OUT_VERTEX_ID_COLUMN = "out_vertex_id";
    private static final String CONFIG_PREFIX = "sql.";

    private final DataSource dataSource;
    private final VertexiumSerializer serializer;

    public SqlGraphConfiguration(Map<String, Object> config) {
        super(config);
        dataSource = createDataSource(config);
        serializer = createSerializer();
    }

    private DataSource createDataSource(Map<String, Object> config) {
        Properties properties = new Properties();
        for (Map.Entry<String, Object> configEntry : config.entrySet()) {
            String key = configEntry.getKey();
            if (key.startsWith(CONFIG_PREFIX)) {
                key = key.substring(CONFIG_PREFIX.length());
                properties.put(key, configEntry.getValue());
            }
        }
        HikariConfig hikariConfig = new HikariConfig(properties);
        return new HikariDataSource(hikariConfig);
    }

    protected DataSource getDataSource() {
        return dataSource;
    }

    protected String tableNameWithPrefix(String tableName) {
        return getTableNamePrefix() + "_" + tableName;
    }
}
