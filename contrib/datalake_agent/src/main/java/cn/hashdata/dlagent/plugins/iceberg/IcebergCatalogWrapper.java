package cn.hashdata.dlagent.plugins.iceberg;

import cn.hashdata.dlagent.api.error.DlRuntimeException;
import cn.hashdata.dlagent.api.error.UnsupportedTypeException;
import cn.hashdata.dlagent.api.model.Metadata;
import cn.hashdata.dlagent.api.model.RequestContext;
import cn.hashdata.dlagent.api.security.SecureLogin;
import cn.hashdata.dlagent.plugins.iceberg.utilities.IcebergUtilities;
import cn.hashdata.dlagent.plugins.hudi.utilities.FilePathUtils;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

@Component
public class IcebergCatalogWrapper {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergCatalogWrapper.class);

    private final Cache<String, IcebergHiveCatalog> catalogCache;

    private IcebergUtilities icebergUtilities;
    private SecureLogin secureLogin;

    public IcebergCatalogWrapper () {
        LOG.info("Creating iceberg catalogCache ...");
        catalogCache = CacheBuilder.newBuilder()
                .removalListener((RemovalListener<String, IcebergHiveCatalog>) notification ->
                        LOG.debug("Removed iceberg catalogCache entry for key {} with cause {}",
                                notification.getKey(),
                                notification.getCause().toString()))
                .build();
    }

    /**
     * Sets the {@link IcebergUtilities} object
     *
     * @param icebergUtilities the iceberg utilities object
     */
    @Autowired
    public void setIcebergUtilities(IcebergUtilities icebergUtilities) {
        this.icebergUtilities = icebergUtilities;
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

    /**
     * Returns the corresponding catalog implementation.
     */
    public IcebergCatalog getIcebergCatalog(RequestContext context) throws Exception {
        String catalogType = context.getCatalogType();
        switch (catalogType) {
            case "hadoop":
                return new IcebergHadoopCatalog(FilePathUtils.unescapePathName(context.getPath()),
                        icebergUtilities, context.getConfiguration());
            case "hive":
                return getHiveCatalog(context);
            default:
                throw new DlRuntimeException("Unexpected catalog type: " + catalogType);
        }
    }

    private String formCatalogCacheKey(RequestContext context) {
        return String.format("%s:%s", context.getServerName(),
                context.getConfiguration().get(HiveConf.ConfVars.METASTOREURIS.varname));
    }

    private IcebergHiveCatalog getHiveCatalog(RequestContext context) throws IOException {
        final String cacheKey = formCatalogCacheKey(context);

        try {
            return catalogCache.get(cacheKey, () -> {
                        LOG.debug("Caching hive catalog with key={}", cacheKey);

                        IcebergHiveCatalog hiveCatalog = new IcebergHiveCatalog(context.getPath(),
                                icebergUtilities,
                                context.getConfiguration(),
                                secureLogin,
                                context.getServerName(),
                                context.getConfig());

                        LOG.info("Returning hive catalog for {} [user={}, table={}.{}, resource={}, path={}, " +
                                        "profile={}, predicate {}available]",
                                cacheKey,
                                context.getUser(),
                                context.getSchemaName(),
                                context.getTableName(),
                                context.getDataSource(),
                                context.getPath(),
                                context.getProfile(),
                                context.hasFilter() ? "" : "un");

                        return hiveCatalog;
                    });
        } catch (UncheckedExecutionException | ExecutionException e) {
            // Unwrap the error
            Exception exception = e.getCause() != null ? (Exception) e.getCause() : e;
            if (exception instanceof IOException)
                throw (IOException) exception;
            throw new IOException(exception);
        }
    }
}
