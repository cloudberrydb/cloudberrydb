package cn.hashdata.dlagent.plugins.hive.utilities;

import cn.hashdata.dlagent.api.security.SecureLogin;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.thrift.TException;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class DlCachedClientPool implements ClientPool<IMetaStoreClient, TException> {
    private static Cache<String, DlHiveClientPool> clientPoolCache;

    private final Configuration conf;
    private final String metastoreUri;
    private final int clientPoolSize;
    private final long evictionInterval;
    private final SecureLogin secureLogin;
    private final String serverName;
    private final String configFile;

    public DlCachedClientPool(Configuration conf,
                       Map<String, String> properties,
                       SecureLogin secureLogin,
                       String serverName,
                       String configFile) {
        this.conf = conf;
        this.metastoreUri = conf.get(HiveConf.ConfVars.METASTOREURIS.varname, "");
        this.clientPoolSize =
                PropertyUtil.propertyAsInt(
                        properties,
                        CatalogProperties.CLIENT_POOL_SIZE,
                        CatalogProperties.CLIENT_POOL_SIZE_DEFAULT);
        this.evictionInterval =
                PropertyUtil.propertyAsLong(
                        properties,
                        CatalogProperties.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS,
                        CatalogProperties.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS_DEFAULT);
        this.secureLogin = secureLogin;
        this.serverName = serverName;
        this.configFile = configFile;
        init();
    }

    DlHiveClientPool clientPool() {
        return clientPoolCache.get(metastoreUri, k -> new DlHiveClientPool(clientPoolSize,
                conf, secureLogin, serverName, configFile));
    }

    private synchronized void init() {
        if (clientPoolCache == null) {
            clientPoolCache =
                    Caffeine.newBuilder()
                            .expireAfterAccess(evictionInterval, TimeUnit.MILLISECONDS)
                            .removalListener((key, value, cause) -> ((DlHiveClientPool) value).close())
                            .build();
        }
    }

    static Cache<String, DlHiveClientPool> clientPoolCache() {
        return clientPoolCache;
    }

    @Override
    public <R> R run(Action<R, IMetaStoreClient, TException> action)
            throws TException, InterruptedException {
        return clientPool().run(action);
    }

    @Override
    public <R> R run(Action<R, IMetaStoreClient, TException> action, boolean retry)
            throws TException, InterruptedException {
        return clientPool().run(action, retry);
    }
}
