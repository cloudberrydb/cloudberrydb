package cn.hashdata.dlagent.api.model;

import cn.hashdata.dlagent.api.utilities.PartitionMetadata;
import lombok.Getter;
import lombok.Setter;


public class Partition {
    /**
     * File path+name, table name, etc.
     */
    @Getter
    private final String sourceName;

    /**
     * Partition metadata information (starting point + length, region location, etc.).
     */
    @Getter
    @Setter
    private PartitionMetadata metadata;

    /**
     * Profile name, recommended for reading given Partition.
     */
    @Getter
    @Setter
    private String profile;

    public Partition(PartitionMetadata metadata) {
        this(null, metadata, null);
    }

    /**
     * Constructs a Partition.
     *
     * @param sourceName the resource uri (File path+name, table name, etc.)
     */
    public Partition(String sourceName) {
        this(sourceName, null);
    }

    /**
     * Constructs a Partition.
     *
     * @param sourceName the resource uri (File path+name, table name, etc.)
     * @param metadata   the metadata for this Partition
     */
    public Partition(String sourceName,
                     PartitionMetadata metadata) {
        this(sourceName, metadata, null);
    }

    /**
     * Contructs a Partition.
     *
     * @param sourceName the resource uri (File path+name, table name, etc.)
     * @param metadata   the metadata for this Partition
     * @param profile    the profile to use for the query
     */
    public Partition(String sourceName,
                     PartitionMetadata metadata,
                     String profile) {
        this.sourceName = sourceName;
        this.metadata = metadata;
        this.profile = profile;
    }
}
