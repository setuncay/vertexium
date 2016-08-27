package org.vertexium.sql;

import org.vertexium.*;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.sql.models.SqlGraphValueBase;
import org.vertexium.sql.utils.RowType;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.EnumSet;

public interface SqlGraphSql {
    Connection getConnection() throws SQLException;

    void createTables();

    void truncate();

    long insertStreamingPropertyValue(Connection conn, StreamingPropertyValue spv);

    void insertElementRow(
            Connection conn,
            ElementType elementType,
            String elementId,
            RowType rowType,
            long timestamp,
            Visibility visibility,
            SqlGraphValueBase value
    );

    Iterable<Edge> selectAllEdges(SqlGraph graph, EnumSet<FetchHint> fetchHints, Long endTime, Authorizations authorizations);

    Iterable<Vertex> selectAllVertices(SqlGraph graph, EnumSet<FetchHint> fetchHints, Long endTime, Authorizations authorizations);

    Iterable<GraphMetadataEntry> metadataSelectAll();

    void metadataSetMetadata(String key, Object value);

    StreamingPropertyValue selectStreamingPropertyValue(long spvRowId);
}
