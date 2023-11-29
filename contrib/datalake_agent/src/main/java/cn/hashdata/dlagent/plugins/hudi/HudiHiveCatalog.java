package cn.hashdata.dlagent.plugins.hudi;

import cn.hashdata.dlagent.api.model.Metadata;
import cn.hashdata.dlagent.api.model.RequestContext;
import cn.hashdata.dlagent.api.security.SecureLogin;
import cn.hashdata.dlagent.plugins.hive.utilities.DlCachedClientPool;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.internal.schema.InternalSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Implementation of HudiCatalog for tables stored in HiveCatalog.
 */
public class HudiHiveCatalog extends HudiBaseCatalog implements HudiCatalog {
    private static final Logger LOG = LoggerFactory.getLogger(HudiHiveCatalog.class);

    private final DlCachedClientPool hiveClients;

    public HudiHiveCatalog(HoodieTableMetaClient metaClient, DlCachedClientPool hiveClients, SecureLogin secureLogin) {
        super(metaClient, secureLogin);
        this.hiveClients = hiveClients;
    }

    @Override
    public Pair<HudiTableOptions, List<CombineHudiSplit>> getSplits(Metadata.Item tableName, RequestContext context) throws Exception {
        return buildInputSplits(context);
    }

    @Override
    public InternalSchema getSchema(Metadata.Item tableName) throws Exception {
        return getTableSchema();
    }
}