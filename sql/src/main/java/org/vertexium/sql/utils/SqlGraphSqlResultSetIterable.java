package org.vertexium.sql.utils;

import org.vertexium.sql.SqlGraphSql;

import java.sql.Connection;
import java.sql.SQLException;

public abstract class SqlGraphSqlResultSetIterable<T> extends ResultSetIterable<T> {
    private final SqlGraphSql sqlGraphSql;

    protected SqlGraphSqlResultSetIterable(
            SqlGraphSql sqlGraphSql,
            PreparedStatementCreator preparedStatementCreator
    ) {
        super(preparedStatementCreator);
        this.sqlGraphSql = sqlGraphSql;
    }

    @Override
    protected final Connection getConnection() throws SQLException {
        return sqlGraphSql.getConnection();
    }
}
