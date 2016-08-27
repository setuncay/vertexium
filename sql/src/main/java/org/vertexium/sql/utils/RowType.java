package org.vertexium.sql.utils;

import org.vertexium.VertexiumException;

public enum RowType {
    SIGNAL(1),
    PROPERTY(2),
    PROPERTY_METADATA(3),
    OUT_EDGE_INFO(4),
    IN_EDGE_INFO(5),
    SOFT_DELETE_PROPERTY(6);

    private final int value;

    RowType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static RowType fromValue(int type) {
        switch (type) {
            case 1:
                return SIGNAL;
            case 2:
                return PROPERTY;
            case 3:
                return PROPERTY_METADATA;
            case 4:
                return OUT_EDGE_INFO;
            case 5:
                return IN_EDGE_INFO;
            case 6:
                return SOFT_DELETE_PROPERTY;
        }
        throw new VertexiumException("Unhandled row type: " + type);
    }
}
