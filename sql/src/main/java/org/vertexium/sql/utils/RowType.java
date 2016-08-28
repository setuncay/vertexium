package org.vertexium.sql.utils;

public enum RowType {
    SIGNAL(1),
    PROPERTY(2),
    PROPERTY_METADATA(3),
    OUT_EDGE_INFO(4),
    IN_EDGE_INFO(5),
    SOFT_DELETE_PROPERTY(6),
    HIDDEN_PROPERTY(7),
    VISIBLE_PROPERTY(8),
    HIDDEN_ELEMENT(9),
    HIDDEN_EDGE_OUT(10),
    HIDDEN_EDGE_IN(11),
    VISIBLE_ELEMENT(12),
    VISIBLE_EDGE_OUT(13),
    VISIBLE_EDGE_IN(14),
    SOFT_DELETE_VERTEX(15),
    SOFT_DELETE_OUT_EDGE(16),
    SOFT_DELETE_IN_EDGE(17),
    SOFT_DELETE_EDGE(18);

    private final int value;

    RowType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
