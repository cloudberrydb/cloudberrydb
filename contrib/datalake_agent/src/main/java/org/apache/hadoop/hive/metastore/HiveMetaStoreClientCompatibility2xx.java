package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.Function;
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
    public List<String> getDatabases(String catName, String databasePattern) throws TException {
        return client.get_databases(databasePattern);
    }

    @Override
    public List<String> getAllDatabases(String catName) throws TException {
        return client.get_all_databases();
    }

    @Override
    public List<Partition> listPartitions(String catName, String db_name, String tbl_name,
                                          int max_parts) throws TException {
        List<Partition> parts = client.get_partitions(db_name, tbl_name, shrinkMaxtoShort(max_parts));
        return deepCopyPartitions(parts);
    }

    @Override
    public PartitionSpecProxy listPartitionSpecs(String catName, String dbName, String tableName,
                                                 int maxParts) throws TException {
        return PartitionSpecProxy.Factory.get(client.get_partitions_pspec(dbName, tableName, maxParts));
    }

    @Override
    public List<Partition> listPartitions(String catName, String db_name, String tbl_name,
                                          List<String> part_vals, int max_parts) throws TException {
        List<Partition> parts = client.get_partitions_ps(db_name,
                tbl_name, part_vals, shrinkMaxtoShort(max_parts));
        return deepCopyPartitions(parts);
    }

    @Override
    public List<Partition> listPartitionsWithAuthInfo(String catName, String dbName, String tableName,
                                                      int maxParts, String userName,
                                                      List<String> groupNames) throws TException {
        List<Partition> parts = client.get_partitions_with_auth(
                dbName, tableName, shrinkMaxtoShort(maxParts), userName, groupNames);
        return deepCopyPartitions(parts);
    }

    @Override
    public List<Partition> listPartitionsWithAuthInfo(String catName, String dbName, String tableName,
                                                      List<String> partialPvals, int maxParts,
                                                      String userName, List<String> groupNames)
            throws TException {
        List<Partition> parts = client.get_partitions_ps_with_auth(
                dbName, tableName, partialPvals, shrinkMaxtoShort(maxParts), userName, groupNames);
        return deepCopyPartitions(parts);
    }

    @Override
    public List<Partition> listPartitionsByFilter(String catName, String db_name, String tbl_name,
                                                  String filter, int max_parts) throws TException {
        List<Partition> parts = client.get_partitions_by_filter(
                db_name, tbl_name, filter, shrinkMaxtoShort(max_parts));
        return deepCopyPartitions(parts);
    }

    @Override
    public PartitionSpecProxy listPartitionSpecsByFilter(String catName, String db_name,
                                                         String tbl_name, String filter,
                                                         int max_parts) throws TException {
        return PartitionSpecProxy.Factory.get(
                client.get_part_specs_by_filter(db_name, tbl_name, filter,
                        max_parts));
    }

    @Override
    public Database getDatabase(String catalogName, String databaseName) throws TException {
        Database d = client.get_database(databaseName);
        return deepCopy(d);
    }

    @Override
    public Partition getPartition(String catName, String dbName, String tblName,
                                  List<String> partVals) throws TException {
        Partition p = client.get_partition(dbName, tblName, partVals);
        return deepCopy(p);
    }

    @Override
    public List<Partition> getPartitionsByNames(String catName, String db_name, String tbl_name,
                                                List<String> part_names) throws TException {
        List<Partition> parts = client.get_partitions_by_names(db_name, tbl_name, part_names);
        return deepCopyPartitions(parts);
    }

    @Override
    public Partition getPartitionWithAuthInfo(String catName, String dbName, String tableName,
                                              List<String> pvals, String userName,
                                              List<String> groupNames) throws TException {
        Partition p = client.get_partition_with_auth(dbName, tableName,
                pvals, userName, groupNames);
        return deepCopy(p);
    }

    @Override
    public List<String> getTables(String catName, String dbName, String tablePattern)
            throws TException {
        return client.get_tables(dbName, tablePattern);
    }

    @Override
    public List<String> getTables(String catName, String dbName, String tablePattern,
                                  TableType tableType) throws TException {
        return client.get_tables_by_type(dbName, tablePattern, tableType.toString());
    }

    @Override
    public List<String> listTableNamesByFilter(String catName, String dbName, String filter,
                                               int maxTables) throws TException {
        return client.get_table_names_by_filter(dbName, filter,
                        shrinkMaxtoShort(maxTables));
    }

    @Override
    public List<String> getMaterializedViewsForRewriting(String catName, String dbname)
            throws MetaException {
        try {
            return client.get_materialized_views_for_rewriting(dbname);
        } catch (Exception e) {
            MetaStoreUtils.logAndThrowMetaException(e);
        }
        return null;
    }

    @Override
    public List<TableMeta> getTableMeta(String catName, String dbPatterns, String tablePatterns,
                                        List<String> tableTypes) throws TException {
        return client.get_table_meta(dbPatterns, tablePatterns, tableTypes);
    }

    @Override
    public List<String> getAllTables(String catName, String dbName) throws TException {
        return client.get_all_tables(dbName);
    }

    @Override
    public List<String> listPartitionNames(String catName, String dbName, String tableName,
                                           int maxParts) throws TException {
        return client.get_partition_names(dbName, tableName, shrinkMaxtoShort(maxParts));
    }

    @Override
    public List<String> listPartitionNames(String catName, String db_name, String tbl_name,
                                           List<String> part_vals, int max_parts) throws TException {
        return client.get_partition_names_ps(db_name, tbl_name, part_vals, shrinkMaxtoShort(max_parts));
    }

    @Override
    public int getNumPartitionsByFilter(String catName, String dbName, String tableName,
                                        String filter) throws TException {
        return client.get_num_partitions_by_filter(dbName, tableName, filter);
    }

    @Override
    public List<FieldSchema> getFields(String catName, String db, String tableName)
            throws TException {
        List<FieldSchema> fields = client.get_fields(db, tableName);
        return deepCopyFieldSchemas(fields);
    }

    @Override
    public List<FieldSchema> getSchema(String catName, String db, String tableName) throws TException {
        EnvironmentContext envCxt = null;
        String addedJars = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.ADDED_JARS);
        if(org.apache.commons.lang.StringUtils.isNotBlank(addedJars)) {
            Map<String, String> props = new HashMap<>();
            props.put("hive.added.jars.path", addedJars);
            envCxt = new EnvironmentContext(props);
        }

        List<FieldSchema> fields = client.get_schema_with_environment_context(db, tableName, envCxt);
        return deepCopyFieldSchemas(fields);
    }

    @Override
    public Partition getPartition(String catName, String dbName, String tblName, String name)
            throws TException {
        Partition p = client.get_partition_by_name(dbName, tblName, name);
        return deepCopy(p);
    }

    @Override
    public Function getFunction(String catName, String dbName, String funcName) throws TException {
        return deepCopy(client.get_function(dbName, funcName));
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
