package cn.hashdata.dlagent.plugins.hive.utilities;

import cn.hashdata.dlagent.api.security.SecureLogin;
import cn.hashdata.dlagent.api.utilities.Utilities;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClientCompatibility1xx;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.ClientPoolImpl;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.hive.HiveClientPool;
import org.apache.iceberg.hive.RuntimeMetaException;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import java.security.PrivilegedExceptionAction;

public class DlHiveClientPool extends ClientPoolImpl<IMetaStoreClient, TException> {
    private static final DynMethods.StaticMethod GET_CLIENT =
            DynMethods.builder("getProxy")
                    .impl(
                            RetryingMetaStoreClient.class,
                            HiveConf.class,
                            HiveMetaHookLoader.class,
                            String.class) // Hive 1 and 2
                    .impl(
                            RetryingMetaStoreClient.class,
                            Configuration.class,
                            HiveMetaHookLoader.class,
                            String.class) // Hive 3
                    .buildStatic();

    private final HiveConf hiveConf;
    private final SecureLogin secureLogin;
    private final String serverName;
    private final String configFile;

    public DlHiveClientPool(int poolSize,
                            Configuration conf,
                            SecureLogin secureLogin,
                            String serverName,
                            String configFile) {
        // Do not allow retry by default as we rely on RetryingHiveClient
        super(poolSize, TTransportException.class, false);
        this.hiveConf = new HiveConf(conf, HiveClientPool.class);
        this.hiveConf.addResource(conf);
        this.secureLogin = secureLogin;
        this.serverName = serverName;
        this.configFile = configFile;
    }

    @Override
    protected IMetaStoreClient newClient() {
        try {
            try {
                if (Utilities.isSecurityEnabled(hiveConf)) {
                    UserGroupInformation loginUser = secureLogin.getLoginUser(serverName, configFile, hiveConf);
                    // wrap in doAs for Kerberos to propagate kerberos tokens from login Subject
                    return loginUser.
                            doAs((PrivilegedExceptionAction<IMetaStoreClient>) () -> GET_CLIENT.invoke(
                                    hiveConf, (HiveMetaHookLoader) tbl -> null, HiveMetaStoreClientCompatibility1xx.class.getName()));
                } else {
                    return GET_CLIENT.invoke(
                            hiveConf, (HiveMetaHookLoader) tbl -> null, HiveMetaStoreClientCompatibility1xx.class.getName());
                }
            } catch (RuntimeException e) {
                // any MetaException would be wrapped into RuntimeException during reflection, so let's
                // double-check type here
                if (e.getCause() instanceof MetaException) {
                    throw (MetaException) e.getCause();
                }
                throw e;
            }
        } catch (MetaException e) {
            throw new RuntimeMetaException(e, "Failed to connect to Hive Metastore");
        } catch (Throwable t) {
            if (t.getMessage().contains("Another instance of Derby may have already booted")) {
                throw new RuntimeMetaException(
                        t,
                        "Failed to start an embedded metastore because embedded "
                                + "Derby supports only one client at a time. To fix this, use a metastore that supports "
                                + "multiple clients.");
            }

            throw new RuntimeMetaException(t, "Failed to connect to Hive Metastore");
        }
    }

    @Override
    protected IMetaStoreClient reconnect(IMetaStoreClient client) {
        try {
            client.close();
            client.reconnect();
        } catch (MetaException e) {
            throw new RuntimeMetaException(e, "Failed to reconnect to Hive Metastore");
        }
        return client;
    }

    @Override
    protected boolean isConnectionException(Exception e) {
        return super.isConnectionException(e)
                || (e != null
                && e instanceof MetaException
                && e.getMessage()
                .contains("Got exception: org.apache.thrift.transport.TTransportException"));
    }

    @Override
    protected void close(IMetaStoreClient client) {
        client.close();
    }

    HiveConf hiveConf() {
        return hiveConf;
    }
}
