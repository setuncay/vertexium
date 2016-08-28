package org.vertexium.sql.models;

import org.vertexium.Visibility;

public class EdgeInVisibleValue extends EdgeInOutVisibleValue {
    private static final long serialVersionUID = 4071664920775677790L;

    public EdgeInVisibleValue(String edgeId, Visibility visibility) {
        super(edgeId, visibility);
    }
}
