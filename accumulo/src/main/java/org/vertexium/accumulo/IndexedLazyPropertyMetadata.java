package org.vertexium.accumulo;

import org.vertexium.Metadata;
import org.vertexium.VertexiumException;
import org.vertexium.VertexiumSerializer;
import org.vertexium.Visibility;
import org.vertexium.id.NameSubstitutionStrategy;

import java.util.List;

public class IndexedLazyPropertyMetadata extends LazyPropertyMetadata {
    private final List<MetadataEntry> metadataEntries;
    private final int[] metadataIndexes;

    public IndexedLazyPropertyMetadata(List<MetadataEntry> metadataEntries, int[] metadataIndexes) {
        this.metadataEntries = metadataEntries;
        this.metadataIndexes = metadataIndexes;
    }

    public Metadata toMetadata(
            VertexiumSerializer vertexiumSerializer,
            NameSubstitutionStrategy nameSubstitutionStrategy
    ) {
        Metadata metadata = new Metadata();
        if (metadataIndexes == null) {
            return metadata;
        }
        for (int metadataIndex : metadataIndexes) {
            MetadataEntry entry = metadataEntries.get(metadataIndex);
            String metadataKey = entry.getMetadataKey(nameSubstitutionStrategy);
            Visibility metadataVisibility = entry.getVisibility();
            Object metadataValue = entry.getValue(vertexiumSerializer);
            if (metadataValue == null) {
                throw new VertexiumException("Invalid metadata value found.");
            }
            metadata.add(metadataKey, metadataValue, metadataVisibility);
        }
        return metadata;
    }
}
