package org.vertexium.sql.utils;

import org.vertexium.VertexiumException;
import org.vertexium.util.CloseableIterator;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.Iterator;

public abstract class ResultSetIterable<T> implements Iterable<T> {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(ResultSetIterable.class);

    @Override
    public Iterator<T> iterator() {
        try {
            Connection conn = getConnection();
            PreparedStatement stmt = getStatement(conn);
            ResultSet resultSet = getResultSet(stmt);
            return new ResultSetIterator(conn, stmt, resultSet);
        } catch (Exception ex) {
            throw new VertexiumException("Could not create iterator", ex);
        }
    }

    protected abstract Connection getConnection() throws SQLException;

    protected abstract PreparedStatement getStatement(Connection conn) throws SQLException;

    protected ResultSet getResultSet(PreparedStatement stmt) throws SQLException {
        return stmt.executeQuery();
    }

    protected abstract T readFromResultSet(ResultSet rs) throws SQLException;

    private class ResultSetIterator implements CloseableIterator<T> {
        private final Connection conn;
        private final Statement stmt;
        private final ResultSet resultSet;
        private T next;
        private T current;

        public ResultSetIterator(Connection conn, Statement stmt, ResultSet resultSet) {
            this.conn = conn;
            this.stmt = stmt;
            this.resultSet = resultSet;
        }

        @Override
        public boolean hasNext() {
            loadNext();
            if (next == null) {
                try {
                    close();
                } catch (Exception e) {
                    LOGGER.error("Could not close " + ResultSetIterable.class.getName(), e);
                }
            }
            return next != null;
        }

        @Override
        public T next() {
            loadNext();
            this.current = this.next;
            this.next = null;
            return this.current;
        }

        public void remove() {
            throw new VertexiumException("not supported");
        }

        private void loadNext() {
            try {
                if (this.next != null) {
                    return;
                }
                if (resultSet.isAfterLast()) {
                    close();
                    return;
                }

                this.next = readFromResultSet(resultSet);
                if (this.next == null) {
                    close();
                }
            } catch (Exception ex) {
                throw new VertexiumException("Could not load next from result set", ex);
            }
        }

        @Override
        public void close() throws IOException {
            try {
                if (!resultSet.isClosed()) {
                    resultSet.close();
                }
            } catch (Exception ex) {
                LOGGER.warn("Could not close result set", ex);
            }
            try {
                if (!stmt.isClosed()) {
                    stmt.close();
                }
            } catch (Exception ex) {
                LOGGER.warn("Could not close statement", ex);
            }
            try {
                if (!conn.isClosed()) {
                    conn.close();
                }
            } catch (Exception ex) {
                LOGGER.error("Could not close connection", ex);
            }
        }
    }
}
