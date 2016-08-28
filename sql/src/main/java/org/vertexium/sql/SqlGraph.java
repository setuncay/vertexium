package org.vertexium.sql;

import org.vertexium.*;
import org.vertexium.event.*;
import org.vertexium.mutation.ExistingElementMutationImpl;
import org.vertexium.mutation.PropertyDeleteMutation;
import org.vertexium.mutation.PropertySoftDeleteMutation;
import org.vertexium.mutation.SetPropertyMetadata;
import org.vertexium.property.MutableProperty;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.search.IndexHint;
import org.vertexium.sql.models.*;
import org.vertexium.sql.utils.RowType;
import org.vertexium.util.IncreasingTime;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import static org.vertexium.util.Preconditions.checkNotNull;

public class SqlGraph extends GraphBaseWithSearchIndex {
    private final SqlGraphSql sqlGraphSql;
    private final GraphMetadataStore metadataStore;

    protected SqlGraph(SqlGraphConfiguration configuration) {
        super(configuration);
        // TODO dynamically create this method from the configuration
        sqlGraphSql = new SqlGraphSqlImpl(getConfiguration(), configuration.createSerializer(this));
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

    @SuppressWarnings("unchecked")
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
                saveVertexBuilder(this, timestampLong);

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

    private void saveVertexBuilder(SqlVertexBuilder vertexBuilder, long timestamp) {
        try (Connection conn = getSqlGraphSql().getConnection()) {
            insertVertexSignalRow(conn, vertexBuilder.getVertexId(), timestamp, vertexBuilder.getVisibility());

// TODO
//        for (PropertyDeleteMutation propertyDeleteMutation : vertexBuilder.getPropertyDeletes()) {
//            addPropertyDeleteToMutation(m, propertyDeleteMutation);
//        }
//        for (PropertySoftDeleteMutation propertySoftDeleteMutation : vertexBuilder.getPropertySoftDeletes()) {
//            addPropertySoftDeleteToMutation(m, propertySoftDeleteMutation);
//        }
            for (Property property : vertexBuilder.getProperties()) {
                ensurePropertyDefined(property.getName(), property.getValue());
                insertElementPropertyRow(conn, ElementType.VERTEX, vertexBuilder.getVertexId(), property);
            }
        } catch (SQLException ex) {
            throw new VertexiumException("Could not save vertex builder: " + vertexBuilder.getVertexId(), ex);
        }
    }

    private void insertElementPropertyRow(Connection conn, ElementType elementType, String elementId, Property property) {
        Object propertyValue = property.getValue();
        if (propertyValue instanceof StreamingPropertyValue) {
            StreamingPropertyValue spv = (StreamingPropertyValue) propertyValue;
            long id = getSqlGraphSql().insertStreamingPropertyValue(conn, spv);
            SqlStreamingPropertyValueRef sspvr = new SqlStreamingPropertyValueRef(id, spv.getValueType(), spv.getLength());
            propertyValue = sspvr;
            if (property instanceof MutableProperty) {
                MutableProperty mutableProperty = (MutableProperty) property;
                mutableProperty.setValue(new SqlStreamingPropertyValue(this, sspvr, spv.getValueType(), spv.getLength()));
            }
        }

        ensurePropertyDefined(property.getName(), propertyValue);

        PropertyValueValue value = new PropertyValueValue(
                property.getKey(),
                property.getName(),
                property.getTimestamp(),
                propertyValue,
                property.getVisibility()
        );
        getSqlGraphSql().insertElementRow(
                conn,
                elementType,
                elementId,
                RowType.PROPERTY,
                property.getTimestamp(),
                property.getVisibility(),
                value
        );

        insertElementPropertyMetadata(conn, elementType, elementId, property);
    }

    private void insertElementPropertyMetadata(Connection conn, ElementType elementType, String elementId, Property property) {
        for (Metadata.Entry entry : property.getMetadata().entrySet()) {
            insertElementPropertyMetadataRow(conn, elementType, elementId, property, entry);
        }
    }

    private void insertElementPropertyMetadataRow(
            Connection conn,
            ElementType elementType,
            String elementId,
            Property property,
            Metadata.Entry metadataEntry
    ) {
        PropertyMetadataValue metadataValue = new PropertyMetadataValue(property, metadataEntry);
        getSqlGraphSql().insertElementRow(
                conn,
                elementType,
                elementId,
                RowType.PROPERTY_METADATA,
                property.getTimestamp(),
                metadataEntry.getVisibility(),
                metadataValue
        );
    }

    @Override
    public Iterable<Vertex> getVertices(EnumSet<FetchHint> fetchHints, Long endTime, Authorizations authorizations) {
        return getSqlGraphSql().selectAllVertices(this, fetchHints, endTime, authorizations);
    }

    @Override
    public Vertex getVertex(String vertexId, EnumSet<FetchHint> fetchHints, Long endTime, Authorizations authorizations) {
        return getSqlGraphSql().selectVertex(this, vertexId, fetchHints, endTime, authorizations);
    }

    @Override
    public EdgeBuilder prepareEdge(
            String edgeId,
            Vertex outVertex,
            Vertex inVertex,
            String label,
            Long timestamp,
            Visibility visibility
    ) {
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
                saveEdgeBuilder(this, timestampLong);

                final SqlEdge edge = createEdge(SqlGraph.this, this, timestampLong, authorizations);
                if (getOutVertex() instanceof SqlVertex) {
                    ((SqlVertex) getOutVertex()).addOutEdgeInfo(edge);
                }
                if (getInVertex() instanceof SqlVertex) {
                    ((SqlVertex) getInVertex()).addInEdgeInfo(edge);
                }
                return savePreparedEdge(this, edge, authorizations);
            }

            @Override
            protected void queueEvent(GraphEvent event) {
                SqlGraph.this.queueEvent(event);
            }
        };
    }

    @Override
    public EdgeBuilderByVertexId prepareEdge(
            String edgeId,
            String outVertexId,
            String inVertexId,
            String label,
            Long timestamp,
            Visibility visibility
    ) {
        if (outVertexId == null) {
            throw new IllegalArgumentException("outVertexId is required");
        }
        if (inVertexId == null) {
            throw new IllegalArgumentException("inVertexId is required");
        }

        if (edgeId == null) {
            edgeId = getIdGenerator().nextId();
        }
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();
        }
        final long timestampLong = timestamp;

        final String finalEdgeId = edgeId;
        return new EdgeBuilderByVertexId(finalEdgeId, outVertexId, inVertexId, label, visibility) {
            @Override
            public Edge save(Authorizations authorizations) {
                // This has to occur before createEdge since it will mutate the properties
                saveEdgeBuilder(this, timestampLong);

                final SqlEdge edge = createEdge(SqlGraph.this, this, timestampLong, authorizations);
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
            EdgeBuilderBase edgeBuilder,
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
    public void softDeleteVertex(Vertex vertex, Long timestamp, Authorizations authorizations) {
        checkNotNull(vertex, "vertex cannot be null");
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();
        }

        getSearchIndex().deleteElement(this, vertex, authorizations);

        // Delete all edges that this vertex participates.
        for (Edge edge : vertex.getEdges(Direction.BOTH, authorizations)) {
            softDeleteEdge(edge, timestamp, authorizations);
        }

        try (Connection conn = getSqlGraphSql().getConnection()) {
            Visibility visibility = vertex.getVisibility();
            getSqlGraphSql().insertElementRow(conn, ElementType.VERTEX, vertex.getId(), RowType.SOFT_DELETE_VERTEX, timestamp, visibility, new SoftDeleteVertexValue());
        } catch (SQLException e) {
            throw new VertexiumException("Could not soft delete vertex", e);
        }

        if (hasEventListeners()) {
            queueEvent(new SoftDeleteVertexEvent(this, vertex));
        }
    }

    @Override
    public void softDeleteEdge(Edge edge, Long timestamp, Authorizations authorizations) {
        checkNotNull(edge, "edge cannot be null");
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();
        }

        getSearchIndex().deleteElement(this, edge, authorizations);

        try (Connection conn = getSqlGraphSql().getConnection()) {
            String outVertexId = edge.getVertexId(Direction.OUT);
            String inVertexId = edge.getVertexId(Direction.IN);
            Visibility visibility = edge.getVisibility();

            getSqlGraphSql().insertElementRow(conn, ElementType.VERTEX, outVertexId, RowType.SOFT_DELETE_OUT_EDGE, timestamp, visibility, new SoftDeleteOutEdgeValue(edge.getId()));
            getSqlGraphSql().insertElementRow(conn, ElementType.VERTEX, inVertexId, RowType.SOFT_DELETE_IN_EDGE, timestamp, visibility, new SoftDeleteInEdgeValue(edge.getId()));
            getSqlGraphSql().insertElementRow(conn, ElementType.EDGE, edge.getId(), RowType.SOFT_DELETE_EDGE, timestamp, visibility, new SoftDeleteEdgeValue());
        } catch (SQLException ex) {
            throw new VertexiumException("Cannot soft delete edge: " + edge.getId(), ex);
        }

        if (hasEventListeners()) {
            queueEvent(new SoftDeleteEdgeEvent(this, edge));
        }
    }

    @Override
    public Iterable<Edge> getEdges(EnumSet<FetchHint> fetchHints, Long endTime, Authorizations authorizations) {
        return getSqlGraphSql().selectAllEdges(this, fetchHints, endTime, authorizations);
    }

    @Override
    public Edge getEdge(String edgeId, EnumSet<FetchHint> fetchHints, Long endTime, Authorizations authorizations) {
        return getSqlGraphSql().selectEdge(this, edgeId, fetchHints, endTime, authorizations);
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
        return authorizations.canRead(visibility);
    }

    @Override
    public void truncate() {
        try {
            getSqlGraphSql().truncate();
            getSearchIndex().truncate(this);
        } catch (Exception ex) {
            throw new VertexiumException("Could not delete rows", ex);
        }
    }

    @Override
    public void markVertexHidden(Vertex vertex, Visibility visibility, Authorizations authorizations) {
        checkNotNull(vertex, "vertex cannot be null");
        long timestamp = IncreasingTime.currentTimeMillis();

        // Delete all edges that this vertex participates.
        for (Edge edge : vertex.getEdges(Direction.BOTH, authorizations)) {
            markEdgeHidden(edge, visibility, authorizations);
        }

        try (Connection conn = getSqlGraphSql().getConnection()) {
            getSqlGraphSql().insertElementRow(
                    conn,
                    ElementType.VERTEX,
                    vertex.getId(),
                    RowType.HIDDEN_ELEMENT,
                    timestamp,
                    visibility,
                    new ElementHiddenValue(visibility)
            );
        } catch (SQLException ex) {
            throw new VertexiumException("Could not mark vertex hidden: " + vertex.getId(), ex);
        }

        if (hasEventListeners()) {
            queueEvent(new MarkHiddenVertexEvent(this, vertex));
        }
    }

    @Override
    public void markVertexVisible(Vertex vertex, Visibility visibility, Authorizations authorizations) {
        checkNotNull(vertex, "vertex cannot be null");
        long timestamp = IncreasingTime.currentTimeMillis();

        // Delete all edges that this vertex participates.
        for (Edge edge : vertex.getEdges(Direction.BOTH, FetchHint.ALL_INCLUDING_HIDDEN, authorizations)) {
            markEdgeVisible(edge, visibility, authorizations);
        }

        try (Connection conn = getSqlGraphSql().getConnection()) {
            getSqlGraphSql().insertElementRow(conn, ElementType.VERTEX, vertex.getId(), RowType.VISIBLE_ELEMENT, timestamp, visibility, new ElementVisibleValue(visibility));
        } catch (SQLException e) {
            throw new VertexiumException("Could not mark vertex visibile", e);
        }

        if (hasEventListeners()) {
            queueEvent(new MarkVisibleVertexEvent(this, vertex));
        }
    }

    @Override
    public void markEdgeHidden(Edge edge, Visibility visibility, Authorizations authorizations) {
        long timestamp = IncreasingTime.currentTimeMillis();

        checkNotNull(edge);

        Vertex out = edge.getVertex(Direction.OUT, authorizations);
        if (out == null) {
            throw new VertexiumException(String.format("Unable to mark edge hidden %s, can't find out vertex %s", edge.getId(), edge.getVertexId(Direction.OUT)));
        }
        Vertex in = edge.getVertex(Direction.IN, authorizations);
        if (in == null) {
            throw new VertexiumException(String.format("Unable to mark edge hidden %s, can't find in vertex %s", edge.getId(), edge.getVertexId(Direction.IN)));
        }

        try (Connection conn = getSqlGraphSql().getConnection()) {
            getSqlGraphSql().insertElementRow(conn, ElementType.VERTEX, out.getId(), RowType.HIDDEN_EDGE_OUT, timestamp, visibility, new EdgeOutHiddenValue(edge.getId()));
            getSqlGraphSql().insertElementRow(conn, ElementType.VERTEX, in.getId(), RowType.HIDDEN_EDGE_IN, timestamp, visibility, new EdgeInHiddenValue(edge.getId()));
            getSqlGraphSql().insertElementRow(conn, ElementType.EDGE, edge.getId(), RowType.HIDDEN_ELEMENT, timestamp, visibility, new ElementHiddenValue(visibility));

            if (out instanceof SqlVertex) {
                ((SqlVertex) out).removeOutEdge(edge.getId());
            }
            if (in instanceof SqlVertex) {
                ((SqlVertex) in).removeInEdge(edge.getId());
            }
        } catch (SQLException e) {
            throw new VertexiumException("Could not hide edge: " + edge.getId(), e);
        }

        if (hasEventListeners()) {
            queueEvent(new MarkHiddenEdgeEvent(this, edge));
        }
    }

    @Override
    public void markEdgeVisible(Edge edge, Visibility visibility, Authorizations authorizations) {
        Vertex out = edge.getVertex(Direction.OUT, FetchHint.ALL_INCLUDING_HIDDEN, authorizations);
        if (out == null) {
            throw new VertexiumException(String.format("Unable to mark edge visible %s, can't find out vertex %s", edge.getId(), edge.getVertexId(Direction.OUT)));
        }
        Vertex in = edge.getVertex(Direction.IN, FetchHint.ALL_INCLUDING_HIDDEN, authorizations);
        if (in == null) {
            throw new VertexiumException(String.format("Unable to mark edge visible %s, can't find in vertex %s", edge.getId(), edge.getVertexId(Direction.IN)));
        }
        long timestamp = IncreasingTime.currentTimeMillis();

        try (Connection conn = getSqlGraphSql().getConnection()) {
            getSqlGraphSql().insertElementRow(conn, ElementType.VERTEX, out.getId(), RowType.VISIBLE_EDGE_OUT, timestamp, visibility, new EdgeOutVisibleValue(edge.getId(), visibility));
            getSqlGraphSql().insertElementRow(conn, ElementType.VERTEX, in.getId(), RowType.VISIBLE_EDGE_IN, timestamp, visibility, new EdgeInVisibleValue(edge.getId(), visibility));
            getSqlGraphSql().insertElementRow(conn, ElementType.EDGE, edge.getId(), RowType.VISIBLE_ELEMENT, timestamp, visibility, new ElementVisibleValue(visibility));
        } catch (SQLException ex) {
            throw new VertexiumException("Could not mark edge visibile", ex);
        }

        if (out instanceof SqlVertex) {
            ((SqlVertex) out).addOutEdge(edge.getId(), edge.getLabel(), in.getId());
        }
        if (in instanceof SqlVertex) {
            ((SqlVertex) in).addInEdge(edge.getId(), edge.getLabel(), out.getId());
        }

        if (hasEventListeners()) {
            queueEvent(new MarkVisibleEdgeEvent(this, edge));
        }
    }

    @Override
    public Authorizations createAuthorizations(String... auths) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public SqlGraphConfiguration getConfiguration() {
        return (SqlGraphConfiguration) super.getConfiguration();
    }

    public SqlGraphSql getSqlGraphSql() {
        return sqlGraphSql;
    }

    void softDeleteProperties(
            SqlElement element,
            Iterable<Property> properties,
            Long timestamp,
            Authorizations authorizations
    ) {
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();
        }

        ElementType elementType = ElementType.getTypeFromElement(element);
        try (Connection conn = getSqlGraphSql().getConnection()) {
            for (Property property : properties) {
                Visibility visibility = property.getVisibility();
                SqlGraphValueBase value = new PropertySoftDeleteValue(property, timestamp);
                getSqlGraphSql().insertElementRow(conn, elementType, element.getId(), RowType.SOFT_DELETE_PROPERTY, timestamp, visibility, value);
            }
        } catch (SQLException ex) {
            throw new VertexiumException("Could not soft delete properties", ex);
        }

        for (Property property : properties) {
            getSearchIndex().deleteProperty(this, element, property, authorizations);

            if (hasEventListeners()) {
                queueEvent(new SoftDeletePropertyEvent(this, element, property));
            }
        }
    }

    void markPropertyHidden(
            SqlElement element,
            Property property,
            Long timestamp,
            Visibility hiddenVisibility
    ) {
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();
        }

        ElementType elementType = ElementType.getTypeFromElement(element);
        try (Connection conn = getSqlGraphSql().getConnection()) {
            PropertyHiddenValue value = new PropertyHiddenValue(property, timestamp, hiddenVisibility);
            getSqlGraphSql().insertElementRow(conn, elementType, element.getId(), RowType.HIDDEN_PROPERTY, timestamp, hiddenVisibility, value);
        } catch (SQLException ex) {
            throw new VertexiumException("Could not soft delete properties", ex);
        }

        if (hasEventListeners()) {
            fireGraphEvent(new MarkHiddenPropertyEvent(this, element, property, hiddenVisibility));
        }
    }

    void markPropertyVisible(
            SqlElement element,
            Property property,
            Long timestamp,
            Visibility hiddenVisibility
    ) {
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();
        }

        ElementType elementType = ElementType.getTypeFromElement(element);
        try (Connection conn = getSqlGraphSql().getConnection()) {
            PropertyVisibleValue value = new PropertyVisibleValue(property, timestamp, hiddenVisibility);
            getSqlGraphSql().insertElementRow(
                    conn,
                    elementType,
                    element.getId(),
                    RowType.HIDDEN_PROPERTY,
                    timestamp, hiddenVisibility,
                    value
            );
        } catch (SQLException ex) {
            throw new VertexiumException("Could not soft delete properties", ex);
        }

        if (hasEventListeners()) {
            fireGraphEvent(new MarkVisiblePropertyEvent(this, element, property, hiddenVisibility));
        }
    }

    private void saveEdgeBuilder(EdgeBuilderBase edgeBuilder, long timestamp) {
        Visibility visibility = edgeBuilder.getVisibility();

        try (Connection conn = getSqlGraphSql().getConnection()) {
            saveToEdgeTable(conn, edgeBuilder, visibility, timestamp);

            String edgeLabel = edgeBuilder.getNewEdgeLabel() != null ? edgeBuilder.getNewEdgeLabel() : edgeBuilder.getLabel();

            saveEdgeInfoOnVertex(
                    conn,
                    edgeBuilder.getEdgeId(),
                    edgeBuilder.getOutVertexId(),
                    edgeBuilder.getInVertexId(),
                    edgeLabel,
                    timestamp,
                    edgeBuilder.getVisibility()
            );
        } catch (SQLException ex) {
            throw new VertexiumException("Could not save edge builder: " + edgeBuilder.getEdgeId(), ex);
        }
    }

    private void saveToEdgeTable(
            Connection conn,
            EdgeBuilderBase edgeBuilder,
            Visibility visibility,
            long timestamp
    ) throws SQLException {
        String edgeId = edgeBuilder.getEdgeId();
        String edgeLabel = edgeBuilder.getLabel();
        if (edgeBuilder.getNewEdgeLabel() != null) {
            edgeLabel = edgeBuilder.getNewEdgeLabel();
            // TODO delete old edge label
            // m.putDelete(AccumuloEdge.CF_SIGNAL, new Text(edgeBuilder.getLabel()), edgeColumnVisibility, currentTimeMillis());
        }

        String outVertexId = edgeBuilder.getOutVertexId();
        String inVertexId = edgeBuilder.getInVertexId();
        insertEdgeSignalRow(conn, edgeId, edgeLabel, outVertexId, inVertexId, timestamp, visibility);
        // TODO m.put(AccumuloEdge.CF_OUT_VERTEX, new Text(edgeBuilder.getOutVertexId()), edgeColumnVisibility, timestamp, ElementMutationBuilder.EMPTY_VALUE);
        // TODO m.put(AccumuloEdge.CF_IN_VERTEX, new Text(edgeBuilder.getInVertexId()), edgeColumnVisibility, timestamp, ElementMutationBuilder.EMPTY_VALUE);

        // TODO
//        for (PropertyDeleteMutation propertyDeleteMutation : edgeBuilder.getPropertyDeletes()) {
//            addPropertyDeleteToMutation(m, propertyDeleteMutation);
//        }
//        for (PropertySoftDeleteMutation propertySoftDeleteMutation : edgeBuilder.getPropertySoftDeletes()) {
//            addPropertySoftDeleteToMutation(m, propertySoftDeleteMutation);
//        }
        for (Property property : edgeBuilder.getProperties()) {
            insertElementPropertyRow(conn, ElementType.EDGE, edgeId, property);
        }
    }

    private void insertEdgeSignalRow(
            Connection conn,
            String edgeId,
            String edgeLabel,
            String outVertexId,
            String inVertexId,
            long timestamp,
            Visibility visibility
    ) {
        EdgeSignalValue value = new EdgeSignalValue(timestamp, edgeLabel, outVertexId, inVertexId, visibility);
        getSqlGraphSql().insertElementRow(conn, ElementType.EDGE, edgeId, RowType.SIGNAL, timestamp, visibility, value);
    }

    private void saveEdgeInfoOnVertex(
            Connection conn,
            String edgeId,
            String outVertexId,
            String inVertexId,
            String edgeLabel,
            long timestamp,
            Visibility edgeVisibility
    ) {
        EdgeInfoValue edgeInfoValue = new EdgeInfoValue(Direction.OUT, edgeId, edgeLabel, inVertexId, edgeVisibility);
        getSqlGraphSql().insertElementRow(conn, ElementType.VERTEX, outVertexId, RowType.OUT_EDGE_INFO, timestamp, edgeVisibility, edgeInfoValue);

        edgeInfoValue = new EdgeInfoValue(Direction.IN, edgeId, edgeLabel, outVertexId, edgeVisibility);
        getSqlGraphSql().insertElementRow(conn, ElementType.VERTEX, inVertexId, RowType.IN_EDGE_INFO, timestamp, edgeVisibility, edgeInfoValue);
    }

    private void insertVertexSignalRow(
            Connection conn,
            String vertexId,
            long timestamp,
            Visibility visibility
    ) throws SQLException {
        VertexSignalValue value = new VertexSignalValue(timestamp, visibility);
        getSqlGraphSql().insertElementRow(conn, ElementType.VERTEX, vertexId, RowType.SIGNAL, timestamp, visibility, value);
    }

    public <TElement extends Element> void saveExistingElementMutation(
            ExistingElementMutationImpl<TElement> mutation,
            Authorizations authorizations
    ) {
        try (Connection conn = getSqlGraphSql().getConnection()) {
            // Order matters a lot here

            // metadata must be altered first because the lookup of a property can include visibility which will be altered by alterElementPropertyVisibilities
            alterPropertyMetadatas(conn, (SqlElement) mutation.getElement(), mutation.getSetPropertyMetadatas());

            // altering properties comes next because alterElementVisibility may alter the vertex and we won't find it
            // TODO
//        getGraph().alterElementPropertyVisibilities(
//                (AccumuloElement) mutation.getElement(),
//                mutation.getAlterPropertyVisibilities(),
//                authorizations
//        );

            Iterable<PropertyDeleteMutation> propertyDeletes = mutation.getPropertyDeletes();
            Iterable<PropertySoftDeleteMutation> propertySoftDeletes = mutation.getPropertySoftDeletes();
            Iterable<Property> properties = mutation.getProperties();

            // TODO
//        overridePropertyTimestamps(properties);

            ((SqlElement) mutation.getElement()).updatePropertiesInternal(properties, propertyDeletes, propertySoftDeletes);
            saveProperties(
                    conn,
                    (SqlElement) mutation.getElement(),
                    properties,
                    propertyDeletes,
                    propertySoftDeletes,
                    mutation.getIndexHint(),
                    authorizations
            );

            // TODO
//        if (mutation.getNewElementVisibility() != null) {
//            getGraph().alterElementVisibility((AccumuloElement) mutation.getElement(), mutation.getNewElementVisibility(), authorizations);
//        }
//
//        if (mutation instanceof EdgeMutation) {
//            EdgeMutation edgeMutation = (EdgeMutation) mutation;
//
//            String newEdgeLabel = edgeMutation.getNewEdgeLabel();
//            if (newEdgeLabel != null) {
//                getGraph().alterEdgeLabel((AccumuloEdge) mutation.getElement(), newEdgeLabel);
//            }
//        }
        } catch (SQLException e) {
            throw new VertexiumException("Could not save existing element mutation", e);
        }
    }

    private void saveProperties(
            Connection conn,
            SqlElement element,
            Iterable<Property> properties,
            Iterable<PropertyDeleteMutation> propertyDeletes,
            Iterable<PropertySoftDeleteMutation> propertySoftDeletes,
            IndexHint indexHint,
            Authorizations authorizations
    ) throws SQLException {
        ElementType elementType = ElementType.getTypeFromElement(element);
        String elementRowKey = element.getId();
        long timestamp = IncreasingTime.currentTimeMillis();

        // TODO
//        for (PropertyDeleteMutation propertyDelete : propertyDeletes) {
//            elementMutationBuilder.addPropertyDeleteToMutation(m, propertyDelete);
//        }
        for (PropertySoftDeleteMutation propertySoftDelete : propertySoftDeletes) {
            SqlGraphValueBase value = new PropertySoftDeleteValue(
                    propertySoftDelete.getKey(),
                    propertySoftDelete.getName(),
                    propertySoftDelete.getVisibility(),
                    timestamp
            );
            getSqlGraphSql().insertElementRow(
                    conn,
                    elementType,
                    element.getId(),
                    RowType.SOFT_DELETE_PROPERTY,
                    timestamp,
                    propertySoftDelete.getVisibility(),
                    value
            );
        }
        for (Property property : properties) {
            insertElementPropertyRow(conn, elementType, elementRowKey, property);
        }

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

    void alterPropertyMetadatas(Connection conn, SqlElement element, List<SetPropertyMetadata> setPropertyMetadatas) {
        if (setPropertyMetadatas.size() == 0) {
            return;
        }

        List<Property> propertiesToSave = new ArrayList<>();
        for (SetPropertyMetadata apm : setPropertyMetadatas) {
            Property property = element.getProperty(apm.getPropertyKey(), apm.getPropertyName(), apm.getPropertyVisibility());
            if (property == null) {
                throw new VertexiumException(String.format("Could not find property %s:%s(%s)", apm.getPropertyKey(), apm.getPropertyName(), apm.getPropertyVisibility()));
            }
            property.getMetadata().add(apm.getMetadataName(), apm.getNewValue(), apm.getMetadataVisibility());
            propertiesToSave.add(property);
        }

        ElementType elementType = ElementType.getTypeFromElement(element);
        for (Property property : propertiesToSave) {
            insertElementPropertyMetadata(conn, elementType, element.getId(), property);
        }
    }

    Iterable<HistoricalPropertyValue> getHistoricalPropertyValues(
            SqlElement sqlElement,
            String key,
            String name,
            Visibility visibility,
            Long startTime,
            Long endTime,
            Authorizations authorizations
    ) {
        throw new VertexiumException("not implemented");
    }

    public void deleteProperty(SqlElement element, Property property, Authorizations authorizations) {
        try (Connection conn = getSqlGraphSql().getConnection()) {
            getSqlGraphSql().deletePropertyRows(
                    conn,
                    ElementType.getTypeFromElement(element),
                    element.getId(),
                    property.getKey(),
                    property.getName(),
                    property.getVisibility()
            );
        } catch (SQLException e) {
            throw new VertexiumException("could not delete property: " + element.getId() + ": " + property, e);
        }

        getSearchIndex().deleteProperty(this, element, property, authorizations);

        if (hasEventListeners()) {
            queueEvent(new DeletePropertyEvent(this, element, property));
        }
    }
}
