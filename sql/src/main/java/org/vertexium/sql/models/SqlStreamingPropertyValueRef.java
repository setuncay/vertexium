package org.vertexium.sql.models;

import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.property.StreamingPropertyValueRef;
import org.vertexium.sql.SqlGraph;

public class SqlStreamingPropertyValueRef extends StreamingPropertyValueRef<SqlGraph> {
    private static final long serialVersionUID = -6437145303252567999L;
    private final long spvRowId;
    private final Class valueType;
    private final long length;

    public SqlStreamingPropertyValueRef(long spvRowId, Class valueType, long length) {
        this.spvRowId = spvRowId;
        this.valueType = valueType;
        this.length = length;
    }

    @Override
    public Class getValueType() {
        return valueType;
    }

    public long getLength() {
        return length;
    }

    @Override
    public StreamingPropertyValue toStreamingPropertyValue(SqlGraph graph) {
        return graph.getSqlGraphSql().selectStreamingPropertyValue(spvRowId);
    }

    @Override
    public String toString() {
        return "SqlStreamingPropertyValueRef{" +
                "spvRowId=" + spvRowId +
                ", valueType=" + valueType +
                ", length=" + length +
                '}';
    }
}
