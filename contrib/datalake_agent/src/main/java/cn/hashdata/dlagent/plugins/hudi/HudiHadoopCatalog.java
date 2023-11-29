package cn.hashdata.dlagent.plugins.hudi;

import cn.hashdata.dlagent.api.model.Metadata;
import cn.hashdata.dlagent.api.model.RequestContext;
import cn.hashdata.dlagent.api.security.SecureLogin;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Implementation of HudiCatalog for tables handled by Hudi's Catalogs API.
 */
public class HudiHadoopCatalog extends HudiBaseCatalog implements HudiCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(HudiHadoopCatalog.class);

    public HudiHadoopCatalog(HoodieTableMetaClient metaClient, SecureLogin secureLogin) {
        super(metaClient, secureLogin);
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

