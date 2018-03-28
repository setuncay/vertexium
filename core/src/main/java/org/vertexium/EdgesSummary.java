package org.vertexium;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.Map;

public class EdgesSummary {
    private final ImmutableMap<String, Integer> outEdgeCountsByLabels;
    private final ImmutableMap<String, Integer> inEdgeCountsByLabels;

    public EdgesSummary(
            ImmutableMap<String, Integer> outEdgeCountsByLabels,
            ImmutableMap<String, Integer> inEdgeCountsByLabels
    ) {
        this.outEdgeCountsByLabels = outEdgeCountsByLabels;
        this.inEdgeCountsByLabels = inEdgeCountsByLabels;
    }

    public EdgesSummary(Map<String, Integer> outEdgeCountsByLabels, Map<String, Integer> inEdgeCountsByLabels) {
        this(
                ImmutableMap.copyOf(outEdgeCountsByLabels),
                ImmutableMap.copyOf(inEdgeCountsByLabels)
        );
    }

    public ImmutableMap<String, Integer> getOutEdgeCountsByLabels() {
        return outEdgeCountsByLabels;
    }

    public ImmutableMap<String, Integer> getInEdgeCountsByLabels() {
        return inEdgeCountsByLabels;
    }

    public ImmutableMap<String, Integer> getEdgeCountsByLabels() {
        Map<String, Integer> m = new HashMap<>(getOutEdgeCountsByLabels());
        for (Map.Entry<String, Integer> entry : getInEdgeCountsByLabels().entrySet()) {
            Integer v = m.get(entry.getKey());
            if (v == null) {
                m.put(entry.getKey(), entry.getValue());
            } else {
                m.put(entry.getKey(), v + entry.getValue());
            }
        }
        return ImmutableMap.copyOf(m);
    }

    public ImmutableSet<String> getOutEdgeLabels() {
        return outEdgeCountsByLabels.keySet();
    }

    public ImmutableSet<String> getInEdgeLabels() {
        return inEdgeCountsByLabels.keySet();
    }

    public ImmutableSet<String> getEdgeLabels() {
        return ImmutableSet.<String>builder()
                .addAll(getOutEdgeLabels())
                .addAll(getInEdgeLabels())
                .build();
    }

    public int getCountOfOutEdges() {
        return outEdgeCountsByLabels.values().stream().mapToInt(l -> l).sum();
    }

    public int getCountOfInEdges() {
        return inEdgeCountsByLabels.values().stream().mapToInt(l -> l).sum();
    }

    public int getCountOfEdges() {
        return getCountOfOutEdges() + getCountOfInEdges();
    }
}
