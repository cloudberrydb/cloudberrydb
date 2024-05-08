package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTablesRequest;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.prependCatalogToDbName;

public class HiveMetaStoreClientCompatibility2xx extends HiveMetaStoreClientCompatibility1xx implements IMetaStoreClient {
    private final Logger LOG = LoggerFactory.getLogger(this.getClass());

    public HiveMetaStoreClientCompatibility2xx(Configuration conf) throws MetaException {
        super(conf);
    }

    public HiveMetaStoreClientCompatibility2xx(Configuration conf, HiveMetaHookLoader hookLoader) throws MetaException {
        super(conf, hookLoader);
    }

    public HiveMetaStoreClientCompatibility2xx(Configuration conf, HiveMetaHookLoader hookLoader, Boolean allowEmbedded) throws MetaException {
        super(conf, hookLoader, allowEmbedded);
    }

    @Override
    public List<String> getDatabases(String databasePattern) throws TException {
        return client.get_databases(databasePattern);
    }

    @Override
    public List<String> getDatabases(String catName, String databasePattern) throws TException {
        return client.get_databases(prependCatalogToDbName(catName, databasePattern, conf));
    }

    @Override
    public List<String> getAllDatabases() throws TException {
        return client.get_all_databases();
    }

    @Override
    public List<String> getAllDatabases(String catName) throws TException {
        return client.get_databases(prependCatalogToDbName(catName, null, conf));
    }

    @Override
    public Table getTable(String catName, String dbName, String tableName) throws TException {
        GetTableRequest req = new GetTableRequest(dbName, tableName);
        req.setCatName(catName);
        req.setCapabilities(VERSION);
        Table t = client.get_table_req(req).getTable();
        return deepCopy(t);
    }

    @Override
    public List<Partition> listPartitions(String db_name, String tbl_name, short max_parts)
            throws TException {
        List<Partition> parts = client.get_partitions(db_name, tbl_name, shrinkMaxtoShort(max_parts));
        return deepCopyPartitions(parts);
    }

    @Override
    public List<Partition> listPartitions(String catName, String db_name, String tbl_name,
                                          int max_parts) throws TException {
        List<Partition> parts = client.get_partitions(prependCatalogToDbName(catName, db_name, conf),
                tbl_name, shrinkMaxtoShort(max_parts));
        return deepCopyPartitions(parts);
    }

    @Override
    public PartitionSpecProxy listPartitionSpecs(String dbName, String tableName, int maxParts) throws TException {
        return PartitionSpecProxy.Factory.get(client.get_partitions_pspec(dbName, tableName, maxParts));
    }

    @Override
    public PartitionSpecProxy listPartitionSpecs(String catName, String dbName, String tableName,
                                                 int maxParts) throws TException {
        return PartitionSpecProxy.Factory.get(client.get_partitions_pspec(prependCatalogToDbName(catName, dbName, conf),
                tableName, maxParts));
    }

    @Override
    public List<Partition> listPartitions(String db_name, String tbl_name,
                                          List<String> part_vals, short max_parts) throws TException {
        List<Partition> parts = client.get_partitions_ps(db_name,
                tbl_name, part_vals, shrinkMaxtoShort(max_parts));
        return deepCopyPartitions(parts);
    }

    @Override
    public List<Partition> listPartitions(String catName, String dbName, String tbl_name,
                                          List<String> part_vals, int max_parts) throws TException {
        List<Partition> parts = client.get_partitions_ps(prependCatalogToDbName(catName, dbName, conf),
                tbl_name, part_vals, shrinkMaxtoShort(max_parts));
        return deepCopyPartitions(parts);
    }

    @Override
    public List<Partition> listPartitionsWithAuthInfo(String dbName, String tableName,
                                                      short maxParts, String userName,
                                                      List<String> groupNames) throws TException {
        List<Partition> parts = client.get_partitions_with_auth(
                dbName, tableName, shrinkMaxtoShort(maxParts), userName, groupNames);
        return deepCopyPartitions(parts);
    }

    @Override
    public List<Partition> listPartitionsWithAuthInfo(String catName, String dbName, String tableName,
                                                      int maxParts, String userName,
                                                      List<String> groupNames) throws TException {
        List<Partition> parts = client.get_partitions_with_auth(prependCatalogToDbName(catName, dbName, conf),
                tableName, shrinkMaxtoShort(maxParts), userName, groupNames);
        return deepCopyPartitions(parts);
    }

    @Override
    public List<Partition> listPartitionsWithAuthInfo(String dbName, String tableName,
                                                      List<String> partialPvals, short maxParts,
                                                      String userName, List<String> groupNames)
            throws TException {
        List<Partition> parts = client.get_partitions_ps_with_auth(
                dbName, tableName, partialPvals, shrinkMaxtoShort(maxParts), userName, groupNames);
        return deepCopyPartitions(parts);
    }

    @Override
    public List<Partition> listPartitionsWithAuthInfo(String catName, String dbName, String tableName,
                                                      List<String> partialPvals, int maxParts,
                                                      String userName, List<String> groupNames)
            throws TException {
        List<Partition> parts = client.get_partitions_ps_with_auth(prependCatalogToDbName(catName, dbName, conf),
                tableName, partialPvals, shrinkMaxtoShort(maxParts), userName, groupNames);
        return deepCopyPartitions(parts);
    }

    @Override
    public List<Partition> listPartitionsByFilter(String db_name, String tbl_name,
                                                  String filter, short max_parts) throws TException {
        List<Partition> parts = client.get_partitions_by_filter(
                db_name, tbl_name, filter, shrinkMaxtoShort(max_parts));
        return deepCopyPartitions(parts);
    }

    @Override
    public List<Partition> listPartitionsByFilter(String catName, String db_name, String tbl_name,
                                                  String filter, int max_parts) throws TException {
        List<Partition> parts = client.get_partitions_by_filter(prependCatalogToDbName(catName, db_name, conf),
                tbl_name, filter, shrinkMaxtoShort(max_parts));
        return deepCopyPartitions(parts);
    }

    @Override
    public PartitionSpecProxy listPartitionSpecsByFilter(String db_name, String tbl_name,
                                                         String filter, int max_parts)
            throws TException {
        return PartitionSpecProxy.Factory.get(
                client.get_part_specs_by_filter(db_name, tbl_name, filter,
                        max_parts));
    }

    @Override
    public PartitionSpecProxy listPartitionSpecsByFilter(String catName, String db_name,
                                                         String tbl_name, String filter,
                                                         int max_parts) throws TException {
        return PartitionSpecProxy.Factory.get(
                client.get_part_specs_by_filter(prependCatalogToDbName(catName, db_name, conf),
                        tbl_name, filter, max_parts));
    }

    @Override
    public Database getDatabase(String name) throws TException {
        Database d = client.get_database(name);
        return deepCopy(d);
    }

    @Override
    public Database getDatabase(String catalogName, String databaseName) throws TException {
        Database d = client.get_database(prependCatalogToDbName(catalogName, databaseName, conf));
        return deepCopy(d);
    }

    @Override
    public Partition getPartition(String db_name, String tbl_name, List<String> part_vals)
            throws TException {
        Partition p = client.get_partition(db_name, tbl_name, part_vals);
        return deepCopy(p);
    }

    @Override
    public Partition getPartition(String catName, String dbName, String tblName,
                                  List<String> partVals) throws TException {
        Partition p = client.get_partition(prependCatalogToDbName(catName, dbName, conf), tblName, partVals);
        return deepCopy(p);
    }

    @Override
    public List<Partition> getPartitionsByNames(String db_name, String tbl_name,
                                                List<String> part_names) throws TException {
        List<Partition> parts = client.get_partitions_by_names(db_name, tbl_name, part_names);
        return deepCopyPartitions(parts);
    }

    @Override
    public List<Partition> getPartitionsByNames(String catName, String db_name, String tbl_name,
                                                List<String> part_names) throws TException {
        List<Partition> parts = client.get_partitions_by_names(prependCatalogToDbName(catName, db_name, conf),
                tbl_name, part_names);
        return deepCopyPartitions(parts);
    }

    @Override
    public Partition getPartitionWithAuthInfo(String db_name, String tbl_name,
                                              List<String> part_vals, String user_name, List<String> group_names)
            throws TException {
        Partition p = client.get_partition_with_auth(db_name, tbl_name,
                part_vals, user_name, group_names);
        return deepCopy(p);
    }

    @Override
    public Partition getPartitionWithAuthInfo(String catName, String dbName, String tableName,
                                              List<String> pvals, String userName,
                                              List<String> groupNames) throws TException {
        Partition p = client.get_partition_with_auth(prependCatalogToDbName(catName, dbName, conf),
                tableName, pvals, userName, groupNames);
        return deepCopy(p);
    }

    @Override
    public List<String> getTables(String dbName, String tablePattern) throws MetaException {
        try {
            return client.get_tables(dbName, tablePattern);
        } catch (Exception e) {
            MetaStoreUtils.logAndThrowMetaException(e);
        }
        return null;
    }

    @Override
    public List<String> getTables(String catName, String dbName, String tablePattern)
            throws TException {
        return client.get_tables(prependCatalogToDbName(catName, dbName, conf), tablePattern);
    }

    @Override
    public List<String> getTables(String dbName, String tablePattern, TableType tableType) throws MetaException {
        try {
            return client.get_tables_by_type(dbName, tablePattern, tableType.toString());
        } catch (Exception e) {
            MetaStoreUtils.logAndThrowMetaException(e);
        }
        return null;
    }

    @Override
    public List<String> getTables(String catName, String dbName, String tablePattern,
                                  TableType tableType) throws TException {
        return client.get_tables_by_type(prependCatalogToDbName(catName, dbName, conf),
                tablePattern, tableType.toString());
    }

    @Override
    public List<Table> getTableObjectsByName(String dbName, List<String> tableNames)
            throws TException {
        return getTableObjectsByName(null, dbName, tableNames);
    }

    @Override
    public List<Table> getTableObjectsByName(String catName, String dbName,
                                             List<String> tableNames) throws TException {
        GetTablesRequest req = new GetTablesRequest(dbName);
        req.setCatName(catName);
        req.setTblNames(tableNames);
        req.setCapabilities(VERSION);
        List<Table> tabs = client.get_table_objects_by_name_req(req).getTables();
        return deepCopyTables(tabs);
    }

    @Override
    public List<String> listTableNamesByFilter(String dbName, String filter, short maxTables)
            throws TException {
        return client.get_table_names_by_filter(dbName, filter,
                shrinkMaxtoShort(maxTables));
    }

    @Override
    public List<String> listTableNamesByFilter(String catName, String dbName, String filter,
                                               int maxTables) throws TException {
        return client.get_table_names_by_filter(prependCatalogToDbName(catName, dbName, conf),
                filter, shrinkMaxtoShort(maxTables));
    }

    @Override
    public List<String> getMaterializedViewsForRewriting(String dbName) throws TException {
        return client.get_materialized_views_for_rewriting(dbName);
    }

    @Override
    public List<String> getMaterializedViewsForRewriting(String catName, String dbname)
            throws MetaException {
        try {
            return client.get_materialized_views_for_rewriting(prependCatalogToDbName(catName, dbname, conf));
        } catch (Exception e) {
            MetaStoreUtils.logAndThrowMetaException(e);
        }
        return null;
    }

    @Override
    public List<TableMeta> getTableMeta(String dbPatterns, String tablePatterns, List<String> tableTypes)
            throws MetaException {
        try {
            return client.get_table_meta(dbPatterns, tablePatterns, tableTypes);
        } catch (Exception e) {
            MetaStoreUtils.logAndThrowMetaException(e);
        }
        return null;
    }

    @Override
    public List<TableMeta> getTableMeta(String catName, String dbPatterns, String tablePatterns,
                                        List<String> tableTypes) throws TException {
        return client.get_table_meta(prependCatalogToDbName(catName, dbPatterns, conf),
                tablePatterns, tableTypes);
    }

    @Override
    public List<String> getAllTables(String dbName) throws MetaException {
        try {
            return client.get_all_tables(dbName);
        } catch (Exception e) {
            MetaStoreUtils.logAndThrowMetaException(e);
        }
        return null;
    }

    @Override
    public List<String> getAllTables(String catName, String dbName) throws TException {
        return client.get_all_tables(prependCatalogToDbName(catName, dbName, conf));
    }

    @Override
    public List<String> listPartitionNames(String dbName, String tblName,
                                           short max) throws NoSuchObjectException, MetaException, TException {
        return client.get_partition_names(dbName, tblName, shrinkMaxtoShort(max));
    }

    @Override
    public List<String> listPartitionNames(String catName, String dbName, String tableName,
                                           int maxParts) throws NoSuchObjectException, MetaException, TException {
        return client.get_partition_names(prependCatalogToDbName(catName, dbName, conf),
                tableName, shrinkMaxtoShort(maxParts));
    }

    @Override
    public List<String> listPartitionNames(String db_name, String tbl_name,
                                           List<String> part_vals, short max_parts) throws TException {
        return client.get_partition_names_ps(db_name, tbl_name, part_vals, shrinkMaxtoShort(max_parts));
    }

    @Override
    public List<String> listPartitionNames(String catName, String db_name, String tbl_name,
                                           List<String> part_vals, int max_parts) throws TException {
        return client.get_partition_names_ps(prependCatalogToDbName(catName, db_name, conf),
                tbl_name, part_vals, shrinkMaxtoShort(max_parts));
    }

    @Override
    public int getNumPartitionsByFilter(String db_name, String tbl_name,
                                        String filter) throws TException {
        return client.get_num_partitions_by_filter(db_name, tbl_name, filter);
    }

    @Override
    public int getNumPartitionsByFilter(String catName, String dbName, String tableName,
                                        String filter) throws TException {
        return client.get_num_partitions_by_filter(prependCatalogToDbName(catName, dbName, conf),
                tableName, filter);
    }

    @Override
    public List<FieldSchema> getFields(String db, String tableName) throws TException {
        List<FieldSchema> fields = client.get_fields(db, tableName);
        return deepCopyFieldSchemas(fields);
    }

    @Override
    public List<FieldSchema> getFields(String catName, String db, String tableName)
            throws TException {
        List<FieldSchema> fields = client.get_fields(prependCatalogToDbName(catName, db, conf), tableName);
        return deepCopyFieldSchemas(fields);
    }

    private EnvironmentContext getEnvContext() {
        EnvironmentContext envCxt = null;
        String addedJars = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.ADDED_JARS);
        if(org.apache.commons.lang.StringUtils.isNotBlank(addedJars)) {
            Map<String, String> props = new HashMap<>();
            props.put("hive.added.jars.path", addedJars);
            envCxt = new EnvironmentContext(props);
        }

        return envCxt;
    }

    @Override
    public List<FieldSchema> getSchema(String db, String tableName) throws TException {
        EnvironmentContext envCxt = getEnvContext();
        List<FieldSchema> fields = client.get_schema_with_environment_context(db, tableName, envCxt);
        return deepCopyFieldSchemas(fields);
    }

    @Override
    public List<FieldSchema> getSchema(String catName, String db, String tableName) throws TException {
        EnvironmentContext envCxt = getEnvContext();
        List<FieldSchema> fields = client.get_schema_with_environment_context(prependCatalogToDbName(
                catName, db, conf), tableName, envCxt);
        return deepCopyFieldSchemas(fields);
    }

    @Override
    public Partition getPartition(String db, String tableName, String partName) throws TException {
        Partition p = client.get_partition_by_name(db, tableName, partName);
        return deepCopy(p);
    }

    @Override
    public Partition getPartition(String catName, String dbName, String tblName, String name)
            throws TException {
        Partition p = client.get_partition_by_name(prependCatalogToDbName(catName, dbName, conf),
                tblName, name);
        return deepCopy(p);
    }

    @Override
    public Function getFunction(String dbName, String funcName) throws TException {
        return deepCopy(client.get_function(dbName, funcName));
    }

    @Override
    public Function getFunction(String catName, String dbName, String funcName) throws TException {
        return deepCopy(client.get_function(prependCatalogToDbName(catName, dbName, conf), funcName));
    }

    @Override
    public List<String> getFunctions(String dbName, String pattern) throws TException {
        return client.get_functions(dbName, pattern);
    }

    @Override
    public List<String> getFunctions(String catName, String dbName, String pattern) throws TException {
        return client.get_functions(prependCatalogToDbName(catName, dbName, conf), pattern);
    }

    private short shrinkMaxtoShort(int max) {
        if (max < 0) {
            return -1;
        } else if (max <= Short.MAX_VALUE) {
            return (short)max;
        } else {
            return Short.MAX_VALUE;
        }
    }

    private List<Table> deepCopyTables(List<Table> tables) {
        List<Table> copy = null;
        if (tables != null) {
            copy = new ArrayList<>();
            for (Table tab : tables) {
                copy.add(deepCopy(tab));
            }
        }
        return copy;
    }

    private List<Partition> deepCopyPartitions(
            Collection<Partition> src, List<Partition> dest) {
        if (src == null) {
            return dest;
        }
        if (dest == null) {
            dest = new ArrayList<Partition>(src.size());
        }
        for (Partition part : src) {
            dest.add(deepCopy(part));
        }
        return dest;
    }

    private Database deepCopy(Database database) {
        Database copy = null;
        if (database != null) {
            copy = new Database(database);
        }
        return copy;
    }

    private Function deepCopy(Function func) {
        Function copy = null;
        if (func != null) {
            copy = new Function(func);
        }
        return copy;
    }

    private List<Partition> deepCopyPartitions(List<Partition> partitions) {
        return deepCopyPartitions(partitions, null);
    }
}
