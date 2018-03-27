package org.vertexium.accumulo.iterator;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;
import org.vertexium.accumulo.iterator.model.EdgeInfo;
import org.vertexium.accumulo.iterator.model.IteratorFetchHints;
import org.vertexium.accumulo.iterator.model.SoftDeleteEdgeInfo;
import org.vertexium.accumulo.iterator.model.VertexElementData;

import java.util.List;
import java.util.Set;

public class VertexIterator extends ElementIterator<VertexElementData> {
    public static final String CF_SIGNAL_STRING = "V";
    public static final Text CF_SIGNAL = new Text(CF_SIGNAL_STRING);
    public static final String CF_OUT_EDGE_STRING = "EOUT";
    public static final Text CF_OUT_EDGE = new Text(CF_OUT_EDGE_STRING);
    public static final String CF_OUT_EDGE_HIDDEN_STRING = "EOUTH";
    public static final Text CF_OUT_EDGE_HIDDEN = new Text(CF_OUT_EDGE_HIDDEN_STRING);
    public static final String CF_OUT_EDGE_SOFT_DELETE_STRING = "EOUTD";
    public static final Text CF_OUT_EDGE_SOFT_DELETE = new Text(CF_OUT_EDGE_SOFT_DELETE_STRING);
    public static final String CF_IN_EDGE_STRING = "EIN";
    public static final Text CF_IN_EDGE = new Text(CF_IN_EDGE_STRING);
    public static final String CF_IN_EDGE_HIDDEN_STRING = "EINH";
    public static final Text CF_IN_EDGE_HIDDEN = new Text(CF_IN_EDGE_HIDDEN_STRING);
    public static final String CF_IN_EDGE_SOFT_DELETE_STRING = "EIND";
    public static final Text CF_IN_EDGE_SOFT_DELETE = new Text(CF_IN_EDGE_SOFT_DELETE_STRING);

    public VertexIterator() {
        this(null);
    }

    public VertexIterator(IteratorFetchHints fetchHints) {
        super(null, fetchHints);
    }

    public VertexIterator(SortedKeyValueIterator<Key, Value> source, IteratorFetchHints fetchHints) {
        super(source, fetchHints);
    }

    @Override
    protected boolean populateElementData(List<Key> keys, List<Value> values) {
        boolean ret = super.populateElementData(keys, values);
        if (ret) {
            removeHiddenAndSoftDeletes();
        }
        return ret;
    }

    private void removeHiddenAndSoftDeletes() {
        if (!getFetchHints().isIncludeHidden()) {
            for (Text edgeId : this.getElementData().hiddenEdges) {
                this.getElementData().inEdges.remove(edgeId);
                this.getElementData().outEdges.remove(edgeId);
            }
        }

        for (SoftDeleteEdgeInfo inSoftDelete : this.getElementData().inSoftDeletes) {
            EdgeInfo inEdge = this.getElementData().inEdges.get(inSoftDelete.getEdgeId());
            if (inEdge != null && inSoftDelete.getTimestamp() >= inEdge.getTimestamp()) {
                this.getElementData().inEdges.remove(inSoftDelete.getEdgeId());
            }
        }

        for (SoftDeleteEdgeInfo outSoftDelete : this.getElementData().outSoftDeletes) {
            EdgeInfo outEdge = this.getElementData().outEdges.get(outSoftDelete.getEdgeId());
            if (outEdge != null && outSoftDelete.getTimestamp() >= outEdge.getTimestamp()) {
                this.getElementData().outEdges.remove(outSoftDelete.getEdgeId());
            }
        }
    }

    @Override
    protected boolean processColumn(Key key, Value value, Text columnFamily, Text columnQualifier) {
        if (CF_OUT_EDGE.equals(columnFamily)) {
            processOutEdge(key.getColumnQualifier(), key.getTimestamp(), value);
            return true;
        }

        if (CF_IN_EDGE.equals(columnFamily)) {
            processInEdge(key.getColumnQualifier(), key.getTimestamp(), value);
            return true;
        }

        if (CF_OUT_EDGE_HIDDEN.equals(columnFamily) || CF_IN_EDGE_HIDDEN.equals(columnFamily)) {
            Text edgeId = key.getColumnQualifier();
            getElementData().hiddenEdges.add(edgeId);
            return true;
        }

        if (CF_IN_EDGE_SOFT_DELETE.equals(columnFamily)) {
            Text edgeId = key.getColumnQualifier();
            getElementData().inSoftDeletes.add(new SoftDeleteEdgeInfo(edgeId, key.getTimestamp()));
            return true;
        }

        if (CF_OUT_EDGE_SOFT_DELETE.equals(columnFamily)) {
            Text edgeId = key.getColumnQualifier();
            getElementData().outSoftDeletes.add(new SoftDeleteEdgeInfo(edgeId, key.getTimestamp()));
            return true;
        }

        return false;
    }

    private void processOutEdge(Text edgeId, long timestamp, Value value) {
        EdgeInfo edgeInfo = EdgeInfo.parse(value, timestamp);
        if (shouldIncludeOutEdge(edgeInfo)) {
            getElementData().outEdges.add(edgeId, edgeInfo);
        }
    }

    private void processInEdge(Text edgeId, long timestamp, Value value) {
        EdgeInfo edgeInfo = EdgeInfo.parse(value, timestamp);
        if (shouldIncludeInEdge(edgeInfo)) {
            getElementData().inEdges.add(edgeId, edgeInfo);
        }
    }

    private boolean shouldIncludeOutEdge(EdgeInfo edgeInfo) {
        Set<String> labels = getFetchHints().getEdgeLabelsOfEdgeRefsToInclude();
        if (labels != null && labels.contains(edgeInfo.getLabel())) {
            return true;
        }

        return getFetchHints().isIncludeAllEdgeRefs()
                || getFetchHints().isIncludeEdgeLabelsAndCounts()
                || getFetchHints().isIncludeOutEdgeRefs();
    }

    private boolean shouldIncludeInEdge(EdgeInfo edgeInfo) {
        Set<String> labels = getFetchHints().getEdgeLabelsOfEdgeRefsToInclude();
        if (labels != null && labels.contains(edgeInfo.getLabel())) {
            return true;
        }

        return getFetchHints().isIncludeAllEdgeRefs()
                || getFetchHints().isIncludeEdgeLabelsAndCounts()
                || getFetchHints().isIncludeInEdgeRefs();
    }

    @Override
    protected Text getVisibilitySignal() {
        return CF_SIGNAL;
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        if (sourceIter != null) {
            return new VertexIterator(sourceIter.deepCopy(env), getFetchHints());
        }
        return new VertexIterator(getFetchHints());
    }

    @Override
    protected VertexElementData createElementData() {
        return new VertexElementData();
    }
}
