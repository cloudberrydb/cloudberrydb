package cn.hashdata.dlagent.plugins.iceberg;

import cn.hashdata.dlagent.api.security.SecureLogin;
import cn.hashdata.dlagent.plugins.iceberg.utilities.IcebergUtilities;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.ConfigProperties;
import org.apache.iceberg.hive.DlIcebergHiveCatalog;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Implementation of IcebergCatalog for tables stored in HiveCatalog.
 */
public class IcebergHiveCatalog implements IcebergCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergHiveCatalog.class);

    private DlIcebergHiveCatalog hiveCatalog;
    private IcebergUtilities icebergUtilities;
    private Configuration configuration;

    public IcebergHiveCatalog(String catalogLocation,
                              IcebergUtilities icebergUtilities,
                              Configuration configuration,
                              SecureLogin secureLogin,
                              String serverName,
                              String configFile) {
        this.icebergUtilities = icebergUtilities;
        this.configuration = configuration;

        HiveConf conf = new HiveConf(configuration, IcebergHiveCatalog.class);
        conf.setBoolean(ConfigProperties.ENGINE_HIVE_ENABLED, true);

        hiveCatalog = new DlIcebergHiveCatalog(secureLogin, serverName, configFile);
        hiveCatalog.setConf(conf);

        Map<String, String> properties = icebergUtilities.composeCatalogProperties(this.configuration);
        if (catalogLocation != null) {
            properties.put(CatalogProperties.WAREHOUSE_LOCATION, catalogLocation);
        }

        properties.put(CatalogProperties.CLIENT_POOL_SIZE, "5");

        hiveCatalog.initialize("IcebergHiveCatalog", properties);
    }

    @Override
    public Table createTable(
            TableIdentifier identifier,
            Schema schema,
            PartitionSpec spec,
            String location,
            Map<String, String> properties) {
        throw new UnsupportedOperationException("Iceberg accessor does not support createTable operation.");
    }

    @Override
    public Table loadTable(String tableName) throws Exception {
        TableIdentifier tableId = icebergUtilities.getIcebergTableIdentifier(tableName);
        return loadTable(tableId, null, null);
    }

    @Override
    public Table loadTable(TableIdentifier tableId, String tableLocation,
                           Map<String, String> properties) throws Exception {
        Preconditions.checkState(tableId != null);
        return hiveCatalog.loadTable(tableId);
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