package cn.hashdata.dlagent.plugins.hudi;

import cn.hashdata.dlagent.api.model.Metadata;
import cn.hashdata.dlagent.api.model.RequestContext;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.common.util.collection.Pair;

import java.util.List;

/**
 * Interface for Hudi catalogs. Only contains a minimal set of methods to make
 * it easy to add support for new Hudi catalogs. Methods that can be implemented in a
 * catalog-agnostic way should be placed in HudiUtil.
 */
public interface HudiCatalog {
    /**
     * Splits files returned by listStatus(JobConf) when they're too big
     */
    Pair<HudiTableOptions, List<CombineHudiSplit>> getSplits(Metadata.Item tableName,
                                                             RequestContext context) throws Exception;

    /**
     * Get the schema of given table
     */
    InternalSchema getSchema(Metadata.Item tableName) throws Exception;
}

