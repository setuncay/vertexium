package org.vertexium.sql.models;

import org.vertexium.Visibility;

public class EdgeOutVisibleValue extends EdgeInOutVisibleValue {
    private static final long serialVersionUID = -5436814199225179009L;

    public EdgeOutVisibleValue(String edgeId, Visibility visibility) {
        super(edgeId, visibility);
    }
}
