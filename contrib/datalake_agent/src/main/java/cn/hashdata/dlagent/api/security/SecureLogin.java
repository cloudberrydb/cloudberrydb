package cn.hashdata.dlagent.api.security;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.security.LoginSession;
import org.apache.hadoop.security.DlUserGroupInformation;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import cn.hashdata.dlagent.api.model.RequestContext;
import cn.hashdata.dlagent.api.utilities.Utilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * This class relies heavily on Hadoop API to
 * <ul>
 * <li>Check need for secure login in Hadoop</li>
 * <li>Parse and load .xml configuration file</li>
 * <li>Do a Kerberos login with a kaytab file</li>
 * <li>convert _HOST in Kerberos principal to current hostname</li>
 * </ul>
 * <p>
 * It uses Hadoop Configuration to parse XML configuration files.<br>
 * It uses Hadoop Security to modify principal and perform the login.
 * <p>
 * The major limitation in this class is its dependency on Hadoop. If Hadoop
 * security is off, no login will be performed regardless of connector being
 * used.
 */
@Component
public class SecureLogin {

    private static final Logger LOG = LoggerFactory.getLogger(SecureLogin.class);

    public static final String CONFIG_KEY_SERVICE_PRINCIPAL = "dlagent.service.kerberos.principal";
    public static final String CONFIG_KEY_SERVICE_KEYTAB = "dlagent.service.kerberos.keytab";
    public static final String CONFIG_KEY_SERVICE_USER_IMPERSONATION = "dlagent.service.user.impersonation";
    public static final String CONFIG_KEY_SERVICE_USER_NAME = "dlagent.service.user.name";

    private static final Map<String, LoginSession> loginMap = new HashMap<>();

    /**
     * Package-private for testing
     */
    private final DlUserGroupInformation dlagentUGI;

    public SecureLogin(DlUserGroupInformation dlagentUGI) {
        this.dlagentUGI = dlagentUGI;
    }


    /**
     * Returns UserGroupInformation for the login user for server specified by the configuration. Tries to re-use
     * existing login user if there was a previous login for this server and no configuration parameters have
     * changed since the last login, otherwise logs the user in and stored the result for future reference.
     *
     * @param context       request context
     * @param configuration location of server configuration directory
     * @return UserGroupInformation of the login user
     * @throws IOException if an error occurs
     */
    public UserGroupInformation getLoginUser(RequestContext context, Configuration configuration) throws IOException {
        return getLoginUser(context.getServerName(), context.getConfig(), configuration);
    }

    /**
     * Returns UserGroupInformation for the login user for the specified server and configuration. Tries to re-use
     * existing login user if there was a previous login for this server and no configuration parameters have
     * changed since the last login, otherwise logs the user in and stored the result for future reference.
     *
     * @param serverName      the name of the server
     * @param configDirectory the location of the configuration directory
     * @param configuration   the configuration for the server
     * @return UserGroupInformation of the login user
     * @throws IOException if an error occurs
     */
    public UserGroupInformation getLoginUser(String serverName,
                                             String configDirectory,
                                             Configuration configuration) throws IOException {
        // Lowercase serverName to keep a single entry in the map
        serverName = serverName.toLowerCase();

        // Kerberos security is enabled for the server, use identity of the Kerberos principal for the server
        LoginSession loginSession = getServerLoginSession(serverName, configDirectory, configuration);
        if (loginSession == null) {
            synchronized (SecureLogin.class) {
                loginSession = getServerLoginSession(serverName, configDirectory, configuration);
                if (loginSession == null) {
                    for (Map.Entry<String, String> entry: configuration) {
                        LOG.info("{}={}", entry.getKey(), entry.getValue());
                    }
                    if (Utilities.isSecurityEnabled(configuration)) {
                        LOG.info("Kerberos Security is enabled for server {}", serverName);
                        loginSession = login(serverName, configDirectory, configuration);
                    } else {
                        // Remote user specified in config file, or defaults to user running dlagent service
                        String remoteUser = configuration.get(CONFIG_KEY_SERVICE_USER_NAME, System.getProperty("user.name"));

                        UserGroupInformation loginUser = UserGroupInformation.createRemoteUser(remoteUser);
                        loginSession = new LoginSession(configDirectory, loginUser);
                    }
                    loginMap.put(serverName, loginSession);
                }
            }
        }

        // try to relogin to keep the TGT token from expiring, if it still has a long validity, it will be a no-op
        if (Utilities.isSecurityEnabled(configuration)) {
            dlagentUGI.reloginFromKeytab(serverName, loginSession);
        }

        return loginSession.getLoginUser();
    }

    /**
     * Establishes login context (LoginSession) for the dlagent service principal using Kerberos keytab.
     *
     * @param serverName      name of the configuration server
     * @param configDirectory path to the configuration directory
     * @param configuration   request configuration
     * @return login session for the server
     */
    private LoginSession login(String serverName, String configDirectory, Configuration configuration) {
        try {
            boolean isUserImpersonationEnabled = isUserImpersonationEnabled(configuration);

            LOG.info("User impersonation is {} for server {}", (isUserImpersonationEnabled ? "enabled" : "disabled"), serverName);

            Configuration config = new Configuration();
            config.set("hadoop.security.authentication", "kerberos");
            UserGroupInformation.reset();
            UserGroupInformation.setConfiguration(config);

            String principal = getServicePrincipal(serverName, configuration);
            String keytabFilename = getServiceKeytab(serverName, configuration);

            if (StringUtils.isEmpty(principal)) {
                throw new RuntimeException(String.format("Kerberos Security for server %s requires a valid principal.", serverName));
            }

            if (StringUtils.isEmpty(keytabFilename)) {
                throw new RuntimeException(String.format("Kerberos Security for server %s requires a valid keytab file name.", serverName));
            }

            configuration.set(CONFIG_KEY_SERVICE_PRINCIPAL, principal);
            configuration.set(CONFIG_KEY_SERVICE_KEYTAB, keytabFilename);

            LOG.info("Kerberos principal for server {}: {}", serverName, principal);
            LOG.info("Kerberos keytab for server {}: {}", serverName, keytabFilename);

            LoginSession loginSession = dlagentUGI
                    .loginUserFromKeytab(configuration, serverName, configDirectory, principal, keytabFilename);

            LOG.info("Logged in as principal {} for server {}", loginSession.getLoginUser(), serverName);

            return loginSession;
        } catch (Exception e) {
            throw new RuntimeException(String.format("dlagent service login failed for server %s : %s", serverName, e.getMessage()), e);
        }
    }

    /**
     * Returns whether user impersonation has been configured as enabled.
     *
     * @return true if user impersonation is enabled, false otherwise
     */
    public boolean isUserImpersonationEnabled(Configuration configuration) {
        return configuration.getBoolean(SecureLogin.CONFIG_KEY_SERVICE_USER_IMPERSONATION, false);
    }

    /**
     * Returns an existing login session for the server if it has already been established before and configuration has not changed.
     *
     * @param serverName      name of the configuration server
     * @param configDirectory path to the configuration directory
     * @param configuration   configuration for the request
     * @return login session or null if it does not exist or does not match current configuration
     */
    private LoginSession getServerLoginSession(final String serverName, final String configDirectory, Configuration configuration) {

        LoginSession currentSession = loginMap.get(serverName);
        if (currentSession == null)
            return null;

        LoginSession expectedLoginSession;
        if (Utilities.isSecurityEnabled(configuration)) {
            long kerberosMinMillisBeforeRelogin = dlagentUGI.getKerberosMinMillisBeforeRelogin(serverName, configuration);
            expectedLoginSession = new LoginSession(
                    configDirectory,
                    getServicePrincipal(serverName, configuration),
                    getServiceKeytab(serverName, configuration),
                    kerberosMinMillisBeforeRelogin);
        } else {
            expectedLoginSession = new LoginSession(configDirectory);
        }

        if (!currentSession.equals(expectedLoginSession)) {
            LOG.warn("LoginSession has changed for server {} : existing {} expected {}", serverName, currentSession, expectedLoginSession);
            return null;
        }

        return currentSession;
    }

    /**
     * Returns the service principal name from the configuration if available,
     * or defaults to the system property for the default server for backwards
     * compatibility. If the prncipal name contains _HOST element, replaces it with the
     * name of the host where the service is running.
     *
     * @param serverName    the name of the server
     * @param configuration the hadoop configuration
     * @return the service principal for the given server and configuration
     */
    String getServicePrincipal(String serverName, Configuration configuration) {
        // use system property as default for backward compatibility when only 1 Kerberized cluster was supported
        String defaultPrincipal = StringUtils.equalsIgnoreCase(serverName, "default") ?
                System.getProperty(CONFIG_KEY_SERVICE_PRINCIPAL) :
                null;
        String principal = configuration.get(CONFIG_KEY_SERVICE_PRINCIPAL, defaultPrincipal);
        try {
            principal = SecurityUtil.getServerPrincipal(principal, getLocalHostName(configuration));
            LOG.debug("Resolved Kerberos principal name to {} for server {}", principal, serverName);
            return principal;
        } catch (Exception e) {
            throw new IllegalStateException(
                    String.format("Failed to determine local hostname for server %s : %s", serverName, e.getMessage()), e);
        }
    }

    /**
     * Returns the service keytab path from the configuration if available,
     * or defaults to the system property for the default server for backwards
     * compatibility.
     *
     * @param serverName    the name of the server
     * @param configuration the hadoop configuration
     * @return the path of the service keytab for the given server and configuration
     */
    String getServiceKeytab(String serverName, Configuration configuration) {
        // use system property as default for backward compatibility when only 1 Kerberized cluster was supported
        String defaultKeytab = StringUtils.equalsIgnoreCase(serverName, "default") ?
                System.getProperty(CONFIG_KEY_SERVICE_KEYTAB) :
                null;
        return configuration.get(CONFIG_KEY_SERVICE_KEYTAB, defaultKeytab);
    }

    /**
     * Retrieve the name of the current host. The code is copied from org.hadoop.security.SecurityUtil class.
     *
     * @param conf configuration
     * @return name of the host
     * @throws IOException when an IOException occurs
     */
    private String getLocalHostName(Configuration conf) throws IOException {
        if (conf != null) {
            String dnsInterface = conf.get("hadoop.security.dns.interface");
            String nameServer = conf.get("hadoop.security.dns.nameserver");
            if (dnsInterface != null) {
                return DNS.getDefaultHost(dnsInterface, nameServer, true);
            }
            if (nameServer != null) {
                throw new IllegalArgumentException("hadoop.security.dns.nameserver requires hadoop.security.dns.interface. Check your configuration.");
            }
        }
        return InetAddress.getLocalHost().getCanonicalHostName();
    }

    /* --------- Testing Only Methods --------- */

    /**
     * Resets and cleans the cache of login sessions. For testing only.
     */
    static void reset() {
        synchronized (SecureLogin.class) {
            loginMap.clear();
        }
    }

    /**
     * Resets and cleans the cache of login sessions. For testing only.
     *
     * @return unmodifiable cache
     */
    static Map<String, LoginSession> getCache() {
        synchronized (SecureLogin.class) {
            return Collections.unmodifiableMap(loginMap);
        }
    }

    /**
     * Adds a given value to cache. For testing only.
     *
     * @param server  server
     * @param session session
     */
    static void addToCache(String server, LoginSession session) {
        synchronized (SecureLogin.class) {
            loginMap.put(server, session);
        }
    }

}
