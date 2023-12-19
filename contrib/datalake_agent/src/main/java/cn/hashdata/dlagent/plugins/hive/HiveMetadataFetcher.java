package cn.hashdata.dlagent.plugins.hive;

import cn.hashdata.dlagent.api.model.BasePlugin;
import cn.hashdata.dlagent.api.model.MetadataFetcher;
import cn.hashdata.dlagent.api.model.Partition;
import cn.hashdata.dlagent.api.model.Metadata;
import cn.hashdata.dlagent.api.model.FragmentDescription;
import cn.hashdata.dlagent.api.utilities.SpringContext;

import cn.hashdata.dlagent.api.security.SecureLogin;
import cn.hashdata.dlagent.plugins.hive.utilities.DlCachedClientPool;
import cn.hashdata.dlagent.plugins.hudi.utilities.FilePathUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;

import cn.hashdata.dlagent.plugins.hive.utilities.HiveUtilities;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.ClientPool;
import org.apache.thrift.TException;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveMetadataFetcher extends BasePlugin implements MetadataFetcher {
    private static final short ALL_PARTS = -1;
    private static final Logger LOG = LoggerFactory.getLogger(HiveMetadataFetcher.class);

    private final HiveClientWrapper hiveClientWrapper;
    protected final HiveUtilities hiveUtilities;
    private final SecureLogin secureLogin;

    private ClientPool<IMetaStoreClient, TException> clients;

    public HiveMetadataFetcher() {
        this(SpringContext.getBean(HiveUtilities.class),
                SpringContext.getBean(HiveClientWrapper.class),
                SpringContext.getBean(SecureLogin.class));
    }

    HiveMetadataFetcher(HiveUtilities hiveUtilities,
                        HiveClientWrapper hiveClientWrapper,
                        SecureLogin secureLogin) {
        this.hiveUtilities = hiveUtilities;
        this.hiveClientWrapper = hiveClientWrapper;
        this.secureLogin = secureLogin;
    }

    @Override
    public void afterPropertiesSet() {
        super.afterPropertiesSet();

        Map<String, String> props = new HashMap<>();
        props.put(CatalogProperties.CLIENT_POOL_SIZE, "5");
        HiveConf conf = new HiveConf(configuration, HiveConf.class);

        this.clients = new DlCachedClientPool(conf,
                                              props,
                                              secureLogin,
                                              context.getServerName(),
                                              context.getConfig());
    }

    public FragmentDescription getFragments(String pattern) throws Exception {
        throw new UnsupportedOperationException("Hive accessor does not support getFragments operation.");
    }

    public List<Partition> getPartitions(String pattern) throws Exception {
        Metadata.Item tblDesc = hiveClientWrapper.extractTableFromName(context.getDataSource());

        return fetchPartitions(tblDesc);
    }

    public Metadata getSchema(String pattern) throws Exception {
        throw new UnsupportedOperationException("Hive accessor does not support getSchema operation.");
    }

    private List<Partition> fetchPartitions(Metadata.Item tblDesc) throws Exception {
        Table table = clients.run(client -> client.getTable(tblDesc.getPath(), tblDesc.getName()));
        String[] partitionKeys = table.getPartitionKeys()
                .stream().map(FieldSchema::getName).toArray(String[]::new);

        Instant startTime = Instant.now();
        List<String> partitions = clients.run(
                client -> client.listPartitionNames(tblDesc.getPath(), tblDesc.getName(), ALL_PARTS));
        Duration duration = Duration.between(startTime, Instant.now());
        LOG.info("Finished listPartitionNames [{}] operation in {} ms.", partitions.size(), duration.toMillis());

        List<Partition> partitionsList = partitions.stream().
                map(p ->
                {List<String> values = FilePathUtils.extractPartitionKeyValues(new org.apache.hadoop.fs.Path(p),
                        true, partitionKeys).values().stream().collect(Collectors.toList());

                    return new Partition(new HivePartitionMetadata(values));
                }).collect(Collectors.toList());

        return partitionsList;
    }

    public boolean open() throws Exception {
        return true;
    }

    public void close() throws Exception {
    }
}
