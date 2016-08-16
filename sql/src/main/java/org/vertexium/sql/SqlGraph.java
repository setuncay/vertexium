package org.vertexium.sql;

import org.vertexium.*;
import org.vertexium.event.AddPropertyEvent;
import org.vertexium.event.AddVertexEvent;
import org.vertexium.event.DeletePropertyEvent;
import org.vertexium.mutation.PropertyDeleteMutation;
import org.vertexium.search.IndexHint;
import org.vertexium.util.IncreasingTime;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.Map;

public class SqlGraph extends GraphBaseWithSearchIndex {
    private final SqlGraphSQL sqlGraphSql;
    private final GraphMetadataStore metadataStore;

    protected SqlGraph(SqlGraphConfiguration configuration) {
        super(configuration);
        sqlGraphSql = new SqlGraphSQL(getConfiguration(), configuration.createSerializer(this));
        metadataStore = new SqlGraphMetadataStore(this);
    }

    public static SqlGraph create(SqlGraphConfiguration config) {
        if (config == null) {
            throw new IllegalArgumentException("config cannot be null");
        }
        SqlGraph graph = new SqlGraph(config);
        graph.setup();
        return graph;
    }

    public static SqlGraph create(Map config) {
        return create(new SqlGraphConfiguration(config));
    }

    @Override
    protected void setup() {
        if (getConfiguration().isCreateTables()) {
            try (Connection conn = getConnection()) {
                sqlGraphSql.createTables(conn);
            } catch (Exception ex) {
                throw new VertexiumException("Could not create tables", ex);
            }
        }
        super.setup();
    }

    @Override
    public void drop() {
        throw new VertexiumException("not implemented");
    }

    @Override
    public VertexBuilder prepareVertex(String vertexId, Long timestamp, Visibility visibility) {
        if (vertexId == null) {
            vertexId = getIdGenerator().nextId();
        }
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();
        }
        final long timestampLong = timestamp;

        final String finalVertexId = vertexId;
        return new SqlVertexBuilder(finalVertexId, visibility) {
            @Override
            public Vertex save(Authorizations authorizations) {
                // This has to occur before createVertex since it will mutate the properties
                getSqlGraphSql().verticesSaveVertexBuilder(SqlGraph.this, this, timestampLong);

                SqlVertex vertex = createVertex(authorizations);

                if (getIndexHint() != IndexHint.DO_NOT_INDEX) {
                    getSearchIndex().addElement(SqlGraph.this, vertex, authorizations);
                }

                if (hasEventListeners()) {
                    queueEvent(new AddVertexEvent(SqlGraph.this, vertex));
                    for (Property property : getProperties()) {
                        queueEvent(new AddPropertyEvent(SqlGraph.this, vertex, property));
                    }
                    for (PropertyDeleteMutation propertyDeleteMutation : getPropertyDeletes()) {
                        queueEvent(new DeletePropertyEvent(SqlGraph.this, vertex, propertyDeleteMutation));
                    }
                }

                return vertex;
            }

            @Override
            protected SqlVertex createVertex(Authorizations authorizations) {
                Iterable<Visibility> hiddenVisibilities = null;
                return new SqlVertex(
                        SqlGraph.this,
                        getVertexId(),
                        getVisibility(),
                        getProperties(),
                        getPropertyDeletes(),
                        getPropertySoftDeletes(),
                        hiddenVisibilities,
                        timestampLong,
                        authorizations
                );
            }
        };
    }

    @Override
    public Iterable<Vertex> getVertices(EnumSet<FetchHint> fetchHints, Long endTime, Authorizations authorizations) {
        return getSqlGraphSql().verticesSelectAll(this, fetchHints, endTime, authorizations);
    }

    @Override
    public EdgeBuilder prepareEdge(String edgeId, Vertex outVertex, Vertex inVertex, String label, Long timestamp, Visibility visibility) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public EdgeBuilderByVertexId prepareEdge(String edgeId, String outVertexId, String inVertexId, String label, Long timestamp, Visibility visibility) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public void softDeleteVertex(Vertex vertex, Long timestamp, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public void softDeleteEdge(Edge edge, Long timestamp, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public Iterable<Edge> getEdges(EnumSet<FetchHint> fetchHints, Long endTime, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    protected GraphMetadataStore getGraphMetadataStore() {
        return metadataStore;
    }

    @Override
    public void deleteVertex(Vertex vertex, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public void deleteEdge(Edge edge, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public boolean isVisibilityValid(Visibility visibility, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public void truncate() {
        throw new VertexiumException("not implemented");
    }

    @Override
    public void markVertexHidden(Vertex vertex, Visibility visibility, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public void markVertexVisible(Vertex vertex, Visibility visibility, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public void markEdgeHidden(Edge edge, Visibility visibility, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public void markEdgeVisible(Edge edge, Visibility visibility, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public Authorizations createAuthorizations(String... auths) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public SqlGraphConfiguration getConfiguration() {
        return (SqlGraphConfiguration) super.getConfiguration();
    }

    public SqlGraphSQL getSqlGraphSql() {
        return sqlGraphSql;
    }

    public Connection getConnection() {
        try {
            return getConfiguration().getDataSource().getConnection();
        } catch (SQLException ex) {
            throw new VertexiumException("Could not get connection", ex);
        }
    }
}
