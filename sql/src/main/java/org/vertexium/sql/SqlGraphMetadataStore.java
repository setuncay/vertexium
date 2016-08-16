package org.vertexium.sql;

import org.vertexium.GraphMetadataEntry;
import org.vertexium.GraphMetadataStore;

public class SqlGraphMetadataStore extends GraphMetadataStore {
    private final SqlGraph sqlGraph;

    public SqlGraphMetadataStore(SqlGraph sqlGraph) {
        this.sqlGraph = sqlGraph;
    }

    @Override
    public Iterable<GraphMetadataEntry> getMetadata() {
        return sqlGraph.getSqlGraphSql().metadataSelectAll();
    }

    @Override
    public void setMetadata(String key, Object value) {
        sqlGraph.getSqlGraphSql().metadataSetMetadata(key, value);
    }
}
