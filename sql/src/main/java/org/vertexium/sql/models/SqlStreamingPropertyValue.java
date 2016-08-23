package org.vertexium.sql.models;

import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.sql.SqlGraph;

import java.io.InputStream;

public class SqlStreamingPropertyValue extends StreamingPropertyValue {
    private static final long serialVersionUID = -3569047572054564557L;
    private final SqlGraph graph;
    private final SqlStreamingPropertyValueRef sspvr;

    public SqlStreamingPropertyValue(
            SqlGraph graph,
            SqlStreamingPropertyValueRef sspvr,
            Class valueType,
            long length
    ) {
        super(null, valueType, length);
        this.graph = graph;
        this.sspvr = sspvr;
    }

    @Override
    public InputStream getInputStream() {
        return sspvr.toStreamingPropertyValue(graph).getInputStream();
    }
}
