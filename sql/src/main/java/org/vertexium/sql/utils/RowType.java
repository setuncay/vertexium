package org.vertexium.sql.utils;

import org.vertexium.VertexiumException;

public enum RowType {
    SIGNAL(1);

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
        }
        throw new VertexiumException("Unhandled row type: " + type);
    }
}
