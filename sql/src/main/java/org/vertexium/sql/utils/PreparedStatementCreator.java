package org.vertexium.sql.utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public interface PreparedStatementCreator {
    PreparedStatement getStatement(Connection conn) throws SQLException;
}
