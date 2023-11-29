package cn.hashdata.dlagent.plugins.iceberg;

import java.io.UncheckedIOException;
import java.util.Map;

import cn.hashdata.dlagent.plugins.iceberg.utilities.IcebergUtilities;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.exceptions.NoSuchTableException;
import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of IcebergCatalog for tables handled by Iceberg's Catalogs API.
 */
public class IcebergHadoopCatalog implements IcebergCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergHadoopCatalog.class);

    private HadoopCatalog hadoopCatalog;
    private IcebergUtilities icebergUtilities;
    private Configuration configuration;

    public IcebergHadoopCatalog(String catalogLocation, IcebergUtilities icebergUtilities, Configuration configuration) {
        this.icebergUtilities = icebergUtilities;
        this.configuration = configuration;
        this.hadoopCatalog = new HadoopCatalog();

        Map<String, String> props = icebergUtilities.composeCatalogProperties(this.configuration);

        String warehouseLocation = String.format("%s/%s", configuration.get("fs.defaultFS"), catalogLocation);
        props.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation);

        LOG.info("warehouse location of iceberg hadoop-table {}", warehouseLocation);

        hadoopCatalog.setConf(this.configuration);
        hadoopCatalog.initialize("", props);
    }

    @Override
    public Table createTable(
            TableIdentifier identifier,
            Schema schema,
            PartitionSpec spec,
            String location,
            Map<String, String> tableProps) throws Exception {
        throw new UnsupportedOperationException("Iceberg accessor does not support createTable operation.");
    }

    @Override
    public Table loadTable(String tableName) throws Exception {
        TableIdentifier tableId = icebergUtilities.getIcebergTableIdentifier(tableName);
        return loadTable(tableId, null, null);
    }

    @Override
    public Table loadTable(TableIdentifier tableId, String tableLocation,
                           Map<String, String> tableProps) throws Exception {
        Preconditions.checkState(tableId != null);
        final int MAX_ATTEMPTS = 5;
        final int SLEEP_MS = 500;
        int attempt = 0;
        while (attempt < MAX_ATTEMPTS) {
            try {
                return hadoopCatalog.loadTable(tableId);
            } catch (NoSuchTableException e) {
                throw new Exception(e.getMessage());
            } catch (NullPointerException | UncheckedIOException e) {
                if (attempt == MAX_ATTEMPTS - 1) {
                    // Throw exception on last attempt.
                    throw e;
                }
                LOG.warn("Caught Exception during Iceberg table loading: {}: {}", tableId, e);
            }
            ++attempt;
            try {
                Thread.sleep(SLEEP_MS);
            } catch (InterruptedException e) {
                // Ignored.
            }
        }
        // We shouldn't really get there, but to make the compiler happy:
        throw new Exception(
                String.format("Failed to load Iceberg table with id: %s", tableId));
    }

    @Override
    public boolean dropTable(String tableName, boolean purge) {
        throw new UnsupportedOperationException("Iceberg accessor does not support dropTable operation.");
    }

    @Override
    public void renameTable(String tableName, String newTableName) {
        throw new UnsupportedOperationException("Iceberg accessor does not support renameTable operation.");
    }
}

