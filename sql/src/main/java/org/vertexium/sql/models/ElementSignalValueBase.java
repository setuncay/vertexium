package org.vertexium.sql.models;

import org.vertexium.Visibility;

public class ElementSignalValueBase extends SqlGraphValueBase {
    private static final long serialVersionUID = 4352884290693475794L;
    private final long timestamp;
    private final String visibility;

    protected ElementSignalValueBase(long timestamp, Visibility visibility) {
        this.timestamp = timestamp;
        this.visibility = visibility.getVisibilityString();
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Visibility getVisibility() {
        return new Visibility(visibility);
    }

    @Override
    public String toString() {
        return "ElementSignalValueBase{" +
                "visibility=" + getVisibility() +
                ", timestamp=" + getTimestamp() +
                '}';
    }
}
