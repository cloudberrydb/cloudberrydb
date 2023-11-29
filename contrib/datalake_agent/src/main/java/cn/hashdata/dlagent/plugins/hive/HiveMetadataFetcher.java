package cn.hashdata.dlagent.plugins.hive;

import cn.hashdata.dlagent.api.model.*;

import cn.hashdata.dlagent.api.security.SecureLogin;
import cn.hashdata.dlagent.plugins.hive.utilities.DlCachedClientPool;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;

import cn.hashdata.dlagent.api.utilities.SpringContext;
import cn.hashdata.dlagent.plugins.hive.utilities.HiveUtilities;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.ClientPool;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HiveMetadataFetcher extends BasePlugin implements MetadataFetcher {
    private static final short ALL_PARTS = -1;
    private static final Log LOG = LogFactory.getLog(HiveMetadataFetcher.class);

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
        List<org.apache.hadoop.hive.metastore.api.Partition> partitions;

        partitions = clients.run(client -> client.listPartitions(tblDesc.getPath(), tblDesc.getName(), ALL_PARTS));

        List<Partition> partitionsList = new ArrayList<>();
        for (org.apache.hadoop.hive.metastore.api.Partition partition : partitions) {
            HivePartitionMetadata metadata = new HivePartitionMetadata(partition.getValues());
            partitionsList.add(new Partition(metadata));
        }

        return partitionsList;
    }

    public boolean open() throws Exception {
        return true;
    }

    public void close() throws Exception {
    }
}
