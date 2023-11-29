package cn.hashdata.dlagent.api.model;

import cn.hashdata.dlagent.api.security.SecureLogin;
import cn.hashdata.dlagent.api.utilities.Utilities;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import cn.hashdata.dlagent.api.configuration.DlServerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTH_TO_LOCAL;

@Component
public class BaseConfigurationFactory implements ConfigurationFactory {
    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private final Map<String, String> hiveOptionMapping = ImmutableMap.of(
            "uris", "hive.metastore.uris",
            "auth_method", "hadoop.security.authentication",
            "krb_service_principal", "hive.metastore.kerberos.principal",
            "krb_client_principal", SecureLogin.CONFIG_KEY_SERVICE_PRINCIPAL,
            "krb_client_keytab", SecureLogin.CONFIG_KEY_SERVICE_KEYTAB);

    private final Map<String, String> hdfsOptionMapping = ImmutableMap.of(
            "hdfs_auth_method", "hadoop.security.authentication",
            "krb_principal", SecureLogin.CONFIG_KEY_SERVICE_PRINCIPAL,
            "krb_principal_keytab", SecureLogin.CONFIG_KEY_SERVICE_KEYTAB,
            "hadoop_rpc_protection","hadoop.rpc.protection",
            "data_transfer_protocol", "dfs.encrypt.data.transfer");
    @Autowired
    public BaseConfigurationFactory(DlServerProperties dlagentServerProperties) {}

    /**
     * {@inheritDoc}
     */
    @Override
    public Configuration initConfiguration(String catalogType,
                                           String configFies,
                                           String serverName,
                                           String userName) {
        // start with built-in Hadoop configuration that loads core-site.xml
        LOG.debug("Initializing configuration for server {}", serverName);
        Configuration configuration = new Configuration(false);
        // while implementing multiple kerberized support we noticed that non-kerberized hadoop
        // access was trying to use SASL-client authentication. Setting the fallback to simple auth
        // allows us to still access non-kerberized hadoop clusters when there exists at least one
        // kerberized hadoop cluster. The root cause is that UGI has static fields and many hadoop
        // libraries depend on the state of the UGI
        // allow using SIMPLE auth for non-Kerberized HCFS access by SASL-enabled IPC client
        // that is created due to the fact that it uses UGI.isSecurityEnabled
        // and will try to use SASL if there is at least one Kerberized Hadoop cluster
        configuration.set(CommonConfigurationKeys.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_KEY, "true");

        // set synthetic property dlagent.session.user so that is can be used in config files for interpolation in other properties
        // for example in JDBC when setting session authorization from a proxy user to the end-user
        configuration.set(DLAGENT_SESSION_USER_PROPERTY, userName);

        // add the server name itself as a configuration property
        configuration.set(DLAGENT_SERVER_NAME_PROPERTY, serverName);

        // add all site files as URL resources to the configuration, no resources will be added from the classpath
        LOG.debug("Using config file {} for server {} configuration", configFies, serverName);
        String[] files = configFies.split("0");
        for (String file : files) {
            processServerResource(catalogType, file, serverName, configuration);
        }

        try {
            // We need to set the restrict system properties to false so
            // variables in the configuration get replaced by system property
            // values
            configuration.setRestrictSystemProps(false);
        } catch (NoSuchMethodError e) {
            // Expected exception for MapR
        }

        // Starting with Hadoop 2.10.0, the "DEFAULT" rule will throw an
        // exception when no rules are applied while getting the principal
        // name translation into operating system user name. See
        // org.apache.hadoop.security.authentication.util.KerberosName#getShortName
        // We add a default rule that will return the service name as the
        // short name, i.e. gpadmin/_HOST@REALM will map to gpadmin
        configuration.set(HADOOP_SECURITY_AUTH_TO_LOCAL, "RULE:[1:$1] RULE:[2:$1] DEFAULT");

        return configuration;
    }

    private void transformOptions(Map<String, Object> serverMap,
                                  Map<String, String> configMap,
                                  Configuration configuration) {
        serverMap.forEach((oldKey, value) -> {
            if (value == null) {
                return;
            }

            String newKey = configMap.get(oldKey);
            if (newKey != null) {
                configuration.set(newKey, value.toString());
                return;
            }

            configuration.set(oldKey, value.toString());
        });
    }

    private void processServerResource(String catalogType,
                                       String configFile,
                                       String serverName,
                                       Configuration configuration) {
        try (InputStream stream = new FileInputStream(configFile)) {
            Yaml yaml = new Yaml();
            Map<String, Map<String, Object>> configMap = yaml.load(stream);
            Map<String, Object> serverConfig = configMap.get(serverName);
            if (serverConfig == null) {
                throw new Exception("server \"" + serverName + "\" not found");
            }

            if (configFile.equals("gphive.conf")) {
                transformHiveConfig(serverConfig, configuration);
            } else {
                transformHdfsConfig(serverConfig, configuration);
            }
        } catch (Exception e) {
            throw new RuntimeException(String.format("Unable to read configuration for server \"%s\" from \"%s\": %s",
                    serverName, configFile, e.toString()));
        }
    }

    private void transformHiveConfig(Map<String, Object> serverMap, Configuration configuration) {
        transformOptions(serverMap, hiveOptionMapping, configuration);
        if (Utilities.isSecurityEnabled(configuration)) {
            configuration.set("hive.metastore.sasl.enabled", "true");
        }
    }

    private void transformHdfsConfig(Map<String, Object> serverMap, Configuration configuration) {
        transformOptions(serverMap, hdfsOptionMapping, configuration);
        configuration.set("ipc.client.connection.maxidletime", "900000");

        if (Utilities.isSecurityEnabled(configuration)) {
            String servicePrincipal = (String) serverMap.get("krb_service_principal");
            if (servicePrincipal != null) {
                configuration.set("dfs.namenode.kerberos.principal", servicePrincipal);
            }

            String transferProtection = (String) serverMap.get("data_transfer_protection");
            if (transferProtection != null) {
                configuration.set("dfs.data.transfer.protection", transferProtection);
            }
        }

        String serviceUser = (String) serverMap.get("service_user_name");
        if (serviceUser != null) {
            configuration.set(SecureLogin.CONFIG_KEY_SERVICE_USER_NAME, serviceUser);
        }

        String enableHa = (String) serverMap.get("is_ha_supported");
        if (enableHa == null) {
            String defaultFs = String.format("hdfs://%s:%s", serverMap.get("hdfs_namenode_host"),
                                              serverMap.get("hdfs_namenode_port"));

            configuration.set("fs.defaultFS", defaultFs);
            return;
        }

        if (enableHa.equals("true")) {
            configuration.set("fs.defaultFS", String.format("hdfs://%s", serverMap.get("dfs.nameservices")));
        }
    }
}
