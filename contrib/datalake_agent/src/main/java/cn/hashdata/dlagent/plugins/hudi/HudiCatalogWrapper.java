package cn.hashdata.dlagent.plugins.hudi;

import cn.hashdata.dlagent.api.error.DlRuntimeException;
import cn.hashdata.dlagent.api.model.Metadata;
import cn.hashdata.dlagent.api.model.RequestContext;
import cn.hashdata.dlagent.api.security.SecureLogin;
import cn.hashdata.dlagent.plugins.hive.utilities.DlCachedClientPool;
import cn.hashdata.dlagent.plugins.hudi.utilities.FilePathUtils;
import cn.hashdata.dlagent.plugins.hudi.utilities.HudiUtilities;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.hadoop.hive.conf.HiveConf;

import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.iceberg.CatalogProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@Component
public class HudiCatalogWrapper {

    private static final Logger LOG = LoggerFactory.getLogger(HudiCatalogWrapper.class);

    private final Cache<String, DlCachedClientPool> catalogCache;

    private HudiUtilities hudiUtilities;
    private SecureLogin secureLogin;

    public HudiCatalogWrapper() {
        LOG.info("Creating hudi catalogCache ...");
        catalogCache = CacheBuilder.newBuilder()
                .removalListener((RemovalListener<String, DlCachedClientPool>) notification ->
                        LOG.debug("Removed hudi catalogCache entry for key {} with cause {}",
                                notification.getKey(),
                                notification.getCause().toString()))
                .build();
    }

    /**
     * Sets the {@link HudiUtilities} object
     *
     * @param hudiUtilities the hudi utilities object
     */
    @Autowired
    public void setHudiUtilities(HudiUtilities hudiUtilities) {
        this.hudiUtilities = hudiUtilities;
    }

    /**
     * Sets the {@link SecureLogin} object
     *
     * @param secureLogin the secure login object
     */
    @Autowired
    public void setSecureLogin(SecureLogin secureLogin) {
        this.secureLogin = secureLogin;
    }

    private HoodieTableMetaClient getMetaClient(RequestContext context) {
        Map<String, String> props = hudiUtilities.composeCatalogProperties(context);

        return HoodieTableMetaClient.builder()
                .setConf(context.getConfiguration())
                .setMetaserverConfig(props)
                .setBasePath(context.getPath()).build();
    }

    /**
     * Returns the corresponding catalog implementation.
     */
    public HudiCatalog getHudiCatalog(Metadata.Item tableName, RequestContext context) throws Exception {
        String catalogType = context.getCatalogType();
        switch (catalogType) {
            case "hadoop":
                context.setPath(String.format("%s/%s",
                        context.getConfiguration().get("fs.defaultFS"),
                        FilePathUtils.unescapePathName(context.getPath())));

                return new HudiHadoopCatalog(getMetaClient(context), secureLogin);
            case "hive":
                DlCachedClientPool clients = getHiveClients(context);
                Table hiveTable = clients.run(client -> client.getTable(tableName.getPath(), tableName.getName()));
                String inputFormat = hiveTable.getSd().getInputFormat();

                if (!inputFormat.equals("org.apache.hudi.hadoop.HoodieParquetInputFormat") &&
                        !inputFormat.equals("org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat")) {
                    throw new Exception("Table \"" + tableName.toString() + "\" is not a hudi table.");
                }

                context.setPath(hiveTable.getSd().getLocation());
                return new HudiHiveCatalog(getMetaClient(context), clients, secureLogin);
            default:
                throw new DlRuntimeException("Unexpected catalog type: " + catalogType);
        }
    }

    private String formCatalogCacheKey(RequestContext context) {
        return String.format("%s:%s", context.getServerName(),
                context.getConfiguration().get(HiveConf.ConfVars.METASTOREURIS.varname));
    }

    private DlCachedClientPool getHiveClients(RequestContext context) throws Exception {
        final String cacheKey = formCatalogCacheKey(context);

        try {
            DlCachedClientPool hiveClients =  catalogCache.get(cacheKey, () -> {
                LOG.debug("Caching hive catalog with key={}", cacheKey);

                Map<String, String> props = new HashMap<>();
                props.put(CatalogProperties.CLIENT_POOL_SIZE, "5");
                HiveConf conf = new HiveConf(context.getConfiguration(), HiveConf.class);

                DlCachedClientPool clients = new DlCachedClientPool(conf, props, secureLogin,
                                                                   context.getServerName(), context.getConfig());

                LOG.info("Returning hive catalog for {} [user={}, table={}.{}, " +
                                "resource={}, path={}, profile={}, predicate {}available]",
                        cacheKey, context.getUser(), context.getSchemaName(), context.getTableName(),
                        context.getDataSource(), context.getPath(), context.getProfile(), context.hasFilter() ? "" : "un");

                return clients;
            });

            return hiveClients;
        } catch (UncheckedExecutionException | ExecutionException e) {
            // Unwrap the error
            Exception exception = e.getCause() != null ? (Exception) e.getCause() : e;
            if (exception instanceof IOException)
                throw (IOException) exception;
            throw new IOException(exception);
        }
    }
}
