package org.vertexium.sql.utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SqlPreparedStatementCreator implements PreparedStatementCreator {
    private final String sql;

    public SqlPreparedStatementCreator(String sql) {
        this.sql = sql;
    }

    @Override
    public PreparedStatement getStatement(Connection conn) throws SQLException {
        return conn.prepareStatement(sql);
    }
}
