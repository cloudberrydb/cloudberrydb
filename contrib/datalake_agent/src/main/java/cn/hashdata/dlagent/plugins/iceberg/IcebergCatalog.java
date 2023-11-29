package cn.hashdata.dlagent.plugins.iceberg;

import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * Interface for Iceberg catalogs. Only contains a minimal set of methods to make
 * it easy to add support for new Iceberg catalogs. Methods that can be implemented in a
 * catalog-agnostic way should be placed in IcebergUtil.
 */
public interface IcebergCatalog {
    /**
     * Creates an Iceberg table in this catalog.
     */
    Table createTable(
            TableIdentifier identifier,
            Schema schema,
            PartitionSpec spec,
            String location,
            Map<String, String> properties) throws Exception;

    Table loadTable(String tableName) throws Exception;

    /**
     * Loads a native Iceberg table based on 'tableId' or 'tableLocation'.
     * @param tableId is the Iceberg table identifier to load the table via the catalog
     *     interface, e.g. HadoopCatalog.
     * @param tableLocation is the filesystem path to load the table via the HadoopTables
     *     interface.
     * @param properties provides information for table loading when Iceberg Catalogs
     *     is being used.
     */
    Table loadTable(TableIdentifier tableId, String tableLocation,
                    Map<String, String> properties) throws Exception;

    /**
     * Drops the table from this catalog.
     * If purge is true, delete all data and metadata files in the table.
     * Return true if the table was dropped, false if the table did not exist
     */
    boolean dropTable(String tableName, boolean purge);

    /**
     * Renames Iceberg table.
     * For HadoopTables, Iceberg does not supported 'renameTable' method
     * For HadoopCatalog, Iceberg implement 'renameTable' method with Exception threw
     */
    void renameTable(String tableName, String newTableName);
}

