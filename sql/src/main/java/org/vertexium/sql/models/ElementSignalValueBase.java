package org.vertexium.sql.models;

import org.vertexium.Visibility;

public class ElementSignalValueBase extends SqlGraphValueBase {
    private static final long serialVersionUID = 4352884290693475794L;
    private final long timestamp;
    private final Visibility visibility;

    protected ElementSignalValueBase(long timestamp, Visibility visibility) {
        this.timestamp = timestamp;
        this.visibility = visibility;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Visibility getVisibility() {
        return visibility;
    }
}
