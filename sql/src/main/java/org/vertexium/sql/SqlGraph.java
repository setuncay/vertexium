package org.vertexium.sql;

import org.vertexium.*;
import org.vertexium.event.AddPropertyEvent;
import org.vertexium.event.DeletePropertyEvent;
import org.vertexium.event.GraphEvent;
import org.vertexium.event.SoftDeletePropertyEvent;
import org.vertexium.mutation.PropertyDeleteMutation;
import org.vertexium.mutation.PropertySoftDeleteMutation;
import org.vertexium.search.IndexHint;
import org.vertexium.util.IncreasingTime;

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
            sqlGraphSql.createTables();
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
                getSqlGraphSql().saveVertexBuilder(SqlGraph.this, this, timestampLong);

                SqlVertex vertex = createVertex(authorizations);

                if (getIndexHint() != IndexHint.DO_NOT_INDEX) {
                    getSearchIndex().addElement(SqlGraph.this, vertex, authorizations);
                }

                if (hasEventListeners()) {
                    notifyEventListeners(SqlGraph.this, vertex);
                }

                return vertex;
            }

            @Override
            protected void queueEvent(GraphEvent event) {
                SqlGraph.this.queueEvent(event);
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
        return getSqlGraphSql().selectAllVertices(this, fetchHints, endTime, authorizations);
    }

    @Override
    public EdgeBuilder prepareEdge(String edgeId, Vertex outVertex, Vertex inVertex, String label, Long timestamp, Visibility visibility) {
        if (outVertex == null) {
            throw new IllegalArgumentException("outVertex is required");
        }
        if (inVertex == null) {
            throw new IllegalArgumentException("inVertex is required");
        }
        if (edgeId == null) {
            edgeId = getIdGenerator().nextId();
        }
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();
        }
        final long timestampLong = timestamp;

        final String finalEdgeId = edgeId;
        return new EdgeBuilder(finalEdgeId, outVertex, inVertex, label, visibility) {
            @Override
            public Edge save(Authorizations authorizations) {
                // This has to occur before createEdge since it will mutate the properties
                getSqlGraphSql().saveEdgeBuilder(this, timestampLong);

                SqlEdge edge = createEdge(SqlGraph.this, this, timestampLong, authorizations);
                if (getOutVertex() instanceof SqlVertex) {
                    ((SqlVertex) getOutVertex()).addOutEdge(edge);
                }
                if (getInVertex() instanceof SqlVertex) {
                    ((SqlVertex) getInVertex()).addInEdge(edge);
                }
                return savePreparedEdge(this, edge, authorizations);
            }

            @Override
            protected void queueEvent(GraphEvent event) {
                SqlGraph.this.queueEvent(event);
            }
        };
    }

    private SqlEdge createEdge(
            SqlGraph graph,
            EdgeBuilder edgeBuilder,
            long timestamp,
            Authorizations authorizations
    ) {
        Iterable<Visibility> hiddenVisibilities = null;
        SqlEdge edge = new SqlEdge(
                graph,
                edgeBuilder.getEdgeId(),
                edgeBuilder.getOutVertexId(),
                edgeBuilder.getInVertexId(),
                edgeBuilder.getLabel(),
                edgeBuilder.getNewEdgeLabel(),
                edgeBuilder.getVisibility(),
                edgeBuilder.getProperties(),
                edgeBuilder.getPropertyDeletes(),
                edgeBuilder.getPropertySoftDeletes(),
                hiddenVisibilities,
                timestamp,
                authorizations
        );
        return edge;
    }


    private Edge savePreparedEdge(
            EdgeBuilderBase edgeBuilder,
            SqlEdge edge,
            Authorizations authorizations
    ) {
        if (edgeBuilder.getIndexHint() != IndexHint.DO_NOT_INDEX) {
            getSearchIndex().addElement(this, edge, authorizations);
        }

        if (hasEventListeners()) {
            edgeBuilder.notifyEventListeners(this, edge);
        }

        return edge;
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
        return getSqlGraphSql().selectAllEdges(this, fetchHints, endTime, authorizations);
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

    public void saveProperties(
            SqlElement element,
            Iterable<Property> properties,
            Iterable<PropertyDeleteMutation> propertyDeletes,
            Iterable<PropertySoftDeleteMutation> propertySoftDeletes,
            IndexHint indexHint,
            Authorizations authorizations
    ) {
        if (indexHint != IndexHint.DO_NOT_INDEX) {
            for (PropertyDeleteMutation propertyDeleteMutation : propertyDeletes) {
                getSearchIndex().deleteProperty(
                        this,
                        element,
                        propertyDeleteMutation.getKey(),
                        propertyDeleteMutation.getName(),
                        propertyDeleteMutation.getVisibility(),
                        authorizations
                );
            }
            for (PropertySoftDeleteMutation propertySoftDeleteMutation : propertySoftDeletes) {
                getSearchIndex().deleteProperty(
                        this,
                        element,
                        propertySoftDeleteMutation.getKey(),
                        propertySoftDeleteMutation.getName(),
                        propertySoftDeleteMutation.getVisibility(),
                        authorizations
                );
            }
            getSearchIndex().addElement(this, element, authorizations);
        }

        if (hasEventListeners()) {
            for (Property property : properties) {
                queueEvent(new AddPropertyEvent(this, element, property));
            }
            for (PropertyDeleteMutation propertyDeleteMutation : propertyDeletes) {
                queueEvent(new DeletePropertyEvent(this, element, propertyDeleteMutation));
            }
            for (PropertySoftDeleteMutation propertySoftDeleteMutation : propertySoftDeletes) {
                queueEvent(new SoftDeletePropertyEvent(this, element, propertySoftDeleteMutation));
            }
        }
    }
}
