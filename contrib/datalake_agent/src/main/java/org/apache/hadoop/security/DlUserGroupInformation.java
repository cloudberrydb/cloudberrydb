package org.apache.hadoop.security;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.apache.hadoop.util.PlatformName;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.security.auth.DestroyFailedException;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_KERBEROS_MIN_SECONDS_BEFORE_RELOGIN;

/**
 * agent-specific analog of <code>UserGroupInformation</code> class to support multiple concurrent Kerberos sessions
 * for different Kerberos Realms and agent configuration servers. The class must to reside on this package to gain access
 * to package-private <code>User</code> class.
 */
@Component
public class DlUserGroupInformation {

    private static final String LOGIN_FAILURE = "Login failure";
    // place logger into cn.hashdata.dlagent.security package to be governed by agent log level, not hadoop one
    private static final Logger LOG = LoggerFactory.getLogger("cn.hashdata.dlagent.security.DlAgentUGI");
    private static final String MUST_FIRST_LOGIN_FROM_KEYTAB = "loginUserFromKeyTab must be done first";
    private static final String OS_LOGIN_MODULE_NAME = getOSLoginModuleName();

    /**
     * Percentage of the ticket window to use before we renew ticket.
     */
    private static final float TICKET_RENEW_WINDOW = 0.80f;

    private static final boolean windows = System.getProperty("os.name").startsWith("Windows");
    private static final boolean is64Bit = System.getProperty("os.arch").contains("64") ||
            System.getProperty("os.arch").contains("s390x");
    private static final boolean aix = System.getProperty("os.name").equals("AIX");

    // package-private LoginContextProvider to allow mocking during testing
    LoginContextProvider loginContextProvider = new LoginContextProvider();

    // package-private Subject provider to allow mock Subjects having User during testing
    Supplier<Subject> subjectProvider = Subject::new;

    // For Tracing purposes to make sure the loginCount and reloginCount remains reasonable
    private final Map<String, AtomicLong> loginCountMap = new HashMap<>();
    private final Map<String, AtomicLong> reloginCountMap = new HashMap<>();

    /**
     * Log a user in from a keytab file. Loads a user identity from a keytab
     * file and logs them in, returning the login session containing user's UGI object.
     *
     * @param configuration   the server configuration
     * @param serverName      the name of the server
     * @param configDirectory the path to the configuration files for the external system
     * @param principal       the principal name to load from the keytab
     * @param keytabFilename  the path to the keytab file
     * @throws IOException           when an IO error occurs.
     * @throws KerberosAuthException if it's a kerberos login exception.
     */
    public synchronized LoginSession loginUserFromKeytab(Configuration configuration, String serverName, String configDirectory, String principal, String keytabFilename) throws IOException {

        Preconditions.checkArgument(StringUtils.isNotBlank(keytabFilename), "Running in secure mode, but config doesn't have a keytab");

        try {
            // create a new Subject to use for login, so that we can remember reference to Subject in LoginSession
            Subject subject = subjectProvider.get();

            // set the configuration on the HadoopKerberosName
            HadoopKerberosName.setConfiguration(configuration);

            long now = Time.now();
            // create login context with the given subject, using Kerberos principal and keytab filename; then login
            LoginContext login = loginContextProvider.newLoginContext(HadoopConfiguration.KEYTAB_KERBEROS_CONFIG_NAME,
                    subject, new HadoopConfiguration(principal, keytabFilename));
            login.login();

            // create UGI with the same Subject used to login, store the login context with the subject's User principal
            UserGroupInformation loginUser = new UserGroupInformation(subject);
            User user = subject.getPrincipals(User.class).iterator().next();
            user.setLogin(login);
            user.setLastLogin(now);
            loginUser.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS);

            LOG.info("Login successful for loginUser {} principal {} using keytab file {}",
                    loginUser, principal, keytabFilename);

            // Keep track of the number of logins per server to make sure
            // we are not logging in too often
            trackEventPerServer(serverName, loginCountMap);

            logStatistics(serverName);

            // store all the relevant information in the login session and return it
            return new LoginSession(configDirectory, principal, keytabFilename, loginUser, subject,
                    getKerberosMinMillisBeforeRelogin(serverName, configuration));

        } catch (LoginException le) {
            KerberosAuthException kae = new KerberosAuthException(LOGIN_FAILURE, le);
            kae.setUser(principal);
            kae.setKeytabFile(keytabFilename);
            throw kae;
        }
    }

    /**
     * Re-Login a user in from a keytab file. Loads a user identity from a keytab
     * file and logs them in. They become the currently logged-in user. This
     * method assumes that {@link #loginUserFromKeytab(Configuration, String, String, String, String)}
     * had happened already.
     * The Subject field of this UserGroupInformation object is updated to have
     * the new credentials.
     *
     * @param serverName   the name of the server
     * @param loginSession the login session
     * @throws IOException           when an IO error occurs
     * @throws KerberosAuthException on a failure
     */
    public void reloginFromKeytab(String serverName, LoginSession loginSession) throws KerberosAuthException {

        UserGroupInformation ugi = loginSession.getLoginUser();

        if (ugi.getAuthenticationMethod() != UserGroupInformation.AuthenticationMethod.KERBEROS ||
                !ugi.isFromKeytab()) {
            LOG.error("Did not attempt to relogin from keytab: auth={}, fromKeyTab={}", ugi.getAuthenticationMethod(), ugi.isFromKeytab());
            return;
        }

        synchronized (loginSession) {
            long now = Time.now();
            if (!hasSufficientTimeElapsed(now, loginSession)) {
                return;
            }

            Subject subject = loginSession.getSubject();
            KerberosTicket tgt = getTGT(subject);
            //Return if TGT is valid and is not going to expire soon.
            if (tgt != null && now < getRefreshTime(tgt)) {
                return;
            }

            User user = loginSession.getUser();
            LoginContext login = user.getLogin();
            String keytabFile = loginSession.getKeytabPath();
            String keytabPrincipal = loginSession.getPrincipalName();

            if (login == null || keytabFile == null) {
                throw new KerberosAuthException(MUST_FIRST_LOGIN_FROM_KEYTAB);
            }

            // register most recent relogin attempt
            user.setLastLogin(now);
            try {
                LOG.debug("Initiating logout for {}", user.getName());
                synchronized (UserGroupInformation.class) {
                    // clear up the kerberos state. But the tokens are not cleared! As per
                    // the Java kerberos login module code, only the kerberos credentials
                    // are cleared
                    login.logout();
                    // login and also update the subject field of this instance to
                    // have the new credentials (pass it to the LoginContext constructor)
                    login = loginContextProvider.newLoginContext(
                            HadoopConfiguration.KEYTAB_KERBEROS_CONFIG_NAME, subject,
                            new HadoopConfiguration(keytabPrincipal, keytabFile));
                    LOG.info("Initiating re-login for {} for server {}", keytabPrincipal, serverName);
                    login.login();
                    fixKerberosTicketOrder(subject);
                    user.setLogin(login);
                }
            } catch (LoginException le) {
                KerberosAuthException kae = new KerberosAuthException(LOGIN_FAILURE, le);
                kae.setPrincipal(keytabPrincipal);
                kae.setKeytabFile(keytabFile);
                throw kae;
            }

            // Keep track of the number of relogins per server to make sure
            // we are not re-logging in too often
            trackEventPerServer(serverName, reloginCountMap);
        }

        logStatistics(serverName);
    }

    public long getKerberosMinMillisBeforeRelogin(String serverName, Configuration configuration) {
        try {
            return 1000L * configuration.getLong(HADOOP_KERBEROS_MIN_SECONDS_BEFORE_RELOGIN, 60L);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(String.format("Invalid attribute value for %s of %s for server %s",
                    HADOOP_KERBEROS_MIN_SECONDS_BEFORE_RELOGIN,
                    configuration.get(HADOOP_KERBEROS_MIN_SECONDS_BEFORE_RELOGIN),
                    serverName));
        }
    }

    // if the first kerberos ticket is not TGT, then remove and destroy it since
    // the kerberos library of jdk always use the first kerberos ticket as TGT.
    // See HADOOP-13433 for more details.
    private void fixKerberosTicketOrder(Subject subject) {
        Set<Object> creds = subject.getPrivateCredentials();
        synchronized (creds) {
            for (Iterator<Object> iter = creds.iterator(); iter.hasNext(); ) {
                Object cred = iter.next();
                if (cred instanceof KerberosTicket) {
                    KerberosTicket ticket = (KerberosTicket) cred;
                    if (ticket.isDestroyed() || ticket.getServer() == null) {
                        LOG.debug("Ticket is already destroyed, remove it.");
                        iter.remove();
                    } else if (!ticket.getServer().getName().startsWith("krbtgt")) {
                        LOG.debug("The first kerberos ticket is not TGT(the server principal is {}), remove and destroy it.",
                                ticket.getServer());
                        iter.remove();
                        try {
                            ticket.destroy();
                        } catch (DestroyFailedException e) {
                            LOG.warn("destroy ticket failed", e);
                        }
                    } else {
                        return;
                    }
                }
            }
        }
        LOG.warn("Warning, no kerberos tickets found while attempting to renew ticket");
    }

    private long getRefreshTime(KerberosTicket tgt) {
        long start = tgt.getStartTime().getTime();
        long end = tgt.getEndTime().getTime();
        return start + (long) ((end - start) * TICKET_RENEW_WINDOW);
    }

    private static String getOSLoginModuleName() {
        if (PlatformName.IBM_JAVA) {
            if (windows) {
                return is64Bit ? "com.ibm.security.auth.module.Win64LoginModule" : "com.ibm.security.auth.module.NTLoginModule";
            } else if (aix) {
                return is64Bit ? "com.ibm.security.auth.module.AIX64LoginModule" : "com.ibm.security.auth.module.AIXLoginModule";
            } else {
                return "com.ibm.security.auth.module.LinuxLoginModule";
            }
        } else {
            return windows ? "com.sun.security.auth.module.NTLoginModule" : "com.sun.security.auth.module.UnixLoginModule";
        }
    }

    /**
     * Get the Kerberos TGT
     *
     * @return the user's TGT or null if none was found
     */
    public synchronized KerberosTicket getTGT(Subject subject) {
        Set<KerberosTicket> tickets = subject.getPrivateCredentials(KerberosTicket.class);
        for (KerberosTicket ticket : tickets) {
            if (SecurityUtil.isOriginalTGT(ticket)) {
                return ticket;
            }
        }
        return null;
    }

    private boolean hasSufficientTimeElapsed(long now, LoginSession loginSession) {
        long lastLogin = loginSession.getUser().getLastLogin();
        long kerberosMinMillisBeforeRelogin = loginSession.getKerberosMinMillisBeforeRelogin();

        if (now - lastLogin < kerberosMinMillisBeforeRelogin) {
            LOG.debug("Not attempting to re-login since the last re-login was attempted less than {} seconds before. Last Login = {}",
                    (kerberosMinMillisBeforeRelogin / 1000), lastLogin);
            return false;
        }
        return true;
    }

    private String prependFileAuthority(String keytabPath) {
        return keytabPath.startsWith("file://") ? keytabPath : "file://" + keytabPath;
    }

    private void trackEventPerServer(String serverName, Map<String, AtomicLong> map) {
        AtomicLong loginCountPerServer = map.get(serverName);

        if (loginCountPerServer == null) {
            loginCountPerServer = new AtomicLong(1L);
            map.put(serverName, loginCountPerServer);
        } else {
            loginCountPerServer.addAndGet(1L);
        }
    }

    private void logStatistics(String serverName) {
        if (!LOG.isTraceEnabled()) return;

        AtomicLong loginPerServer = loginCountMap.get(serverName);
        AtomicLong reloginPerServer = reloginCountMap.get(serverName);

        long loginPerServerValue = loginPerServer != null ? loginPerServer.get() : 0;
        long reloginPerServerValue = reloginPerServer != null ? reloginPerServer.get() : 0;

        LOG.trace("DlAgentUGI metrics: {} login{} and {} relogin{} for server {}",
                loginPerServerValue,
                loginPerServerValue == 1 ? "" : "s",
                reloginPerServerValue,
                reloginPerServerValue == 1 ? "" : "s",
                serverName
        );
    }

    static class LoginContextProvider {

        LoginContext newLoginContext(String appName, Subject subject, javax.security.auth.login.Configuration loginConf)
                throws LoginException {
            // Temporarily switch the thread's ContextClassLoader to match this
            // class's classloader, so that we can properly load HadoopLoginModule
            // from the JAAS libraries.
            Thread t = Thread.currentThread();
            ClassLoader oldCCL = t.getContextClassLoader();
            t.setContextClassLoader(UserGroupInformation.HadoopLoginModule.class.getClassLoader());
            try {
                return new LoginContext(appName, subject, null, loginConf);
            } finally {
                t.setContextClassLoader(oldCCL);
            }
        }
    }

    /**
     * This class is copied from <code>UserGroupInformation</code> class except it requires specific Kerberos
     * principal and keytab, unlike in the source class, where those are static and thus common to all servers.
     */
    class HadoopConfiguration extends javax.security.auth.login.Configuration {
        private static final String KEYTAB_KERBEROS_CONFIG_NAME = "hadoop-keytab-kerberos";
        private final Map<String, String> BASIC_JAAS_OPTIONS = new HashMap<>();
        private final AppConfigurationEntry OS_SPECIFIC_LOGIN;
        private final AppConfigurationEntry HADOOP_LOGIN;
        private final Map<String, String> USER_KERBEROS_OPTIONS;
        private final AppConfigurationEntry USER_KERBEROS_LOGIN;
        private final Map<String, String> KEYTAB_KERBEROS_OPTIONS;
        private final AppConfigurationEntry KEYTAB_KERBEROS_LOGIN;
        private final AppConfigurationEntry[] SIMPLE_CONF;
        private final AppConfigurationEntry[] USER_KERBEROS_CONF;
        private final AppConfigurationEntry[] KEYTAB_KERBEROS_CONF;

        private final String keytabPrincipal;
        private final String keytabFile;

        private HadoopConfiguration(String keytabPrincipal, String keytabFile) {
            this.keytabFile = keytabFile;
            this.keytabPrincipal = keytabPrincipal;

            String ticketCache = System.getenv("HADOOP_JAAS_DEBUG");
            if ("true".equalsIgnoreCase(ticketCache)) {
                BASIC_JAAS_OPTIONS.put("debug", "true");
            }

            OS_SPECIFIC_LOGIN = new AppConfigurationEntry(OS_LOGIN_MODULE_NAME, AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, BASIC_JAAS_OPTIONS);
            HADOOP_LOGIN = new AppConfigurationEntry(UserGroupInformation.HadoopLoginModule.class.getName(), AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, BASIC_JAAS_OPTIONS);
            USER_KERBEROS_OPTIONS = new HashMap<>();
            if (PlatformName.IBM_JAVA) {
                USER_KERBEROS_OPTIONS.put("useDefaultCcache", "true");
            } else {
                USER_KERBEROS_OPTIONS.put("doNotPrompt", "true");
                USER_KERBEROS_OPTIONS.put("useTicketCache", "true");
            }

            ticketCache = System.getenv("KRB5CCNAME");
            if (ticketCache != null) {
                if (PlatformName.IBM_JAVA) {
                    System.setProperty("KRB5CCNAME", ticketCache);
                } else {
                    USER_KERBEROS_OPTIONS.put("ticketCache", ticketCache);
                }
            }

            USER_KERBEROS_OPTIONS.put("renewTGT", "true");
            USER_KERBEROS_OPTIONS.putAll(BASIC_JAAS_OPTIONS);
            USER_KERBEROS_LOGIN = new AppConfigurationEntry(KerberosUtil.getKrb5LoginModuleName(), AppConfigurationEntry.LoginModuleControlFlag.OPTIONAL, USER_KERBEROS_OPTIONS);
            KEYTAB_KERBEROS_OPTIONS = new HashMap<>();
            if (PlatformName.IBM_JAVA) {
                KEYTAB_KERBEROS_OPTIONS.put("credsType", "both");
            } else {
                KEYTAB_KERBEROS_OPTIONS.put("doNotPrompt", "true");
                KEYTAB_KERBEROS_OPTIONS.put("useKeyTab", "true");
                KEYTAB_KERBEROS_OPTIONS.put("storeKey", "true");
            }

            KEYTAB_KERBEROS_OPTIONS.put("refreshKrb5Config", "true");
            KEYTAB_KERBEROS_OPTIONS.putAll(BASIC_JAAS_OPTIONS);
            KEYTAB_KERBEROS_LOGIN = new AppConfigurationEntry(KerberosUtil.getKrb5LoginModuleName(), AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, KEYTAB_KERBEROS_OPTIONS);
            SIMPLE_CONF = new AppConfigurationEntry[]{OS_SPECIFIC_LOGIN, HADOOP_LOGIN};
            USER_KERBEROS_CONF = new AppConfigurationEntry[]{OS_SPECIFIC_LOGIN, USER_KERBEROS_LOGIN, HADOOP_LOGIN};
            KEYTAB_KERBEROS_CONF = new AppConfigurationEntry[]{KEYTAB_KERBEROS_LOGIN, HADOOP_LOGIN};
        }

        public AppConfigurationEntry[] getAppConfigurationEntry(String appName) {
            if ("hadoop-simple".equals(appName)) {
                return SIMPLE_CONF;
            } else if ("hadoop-user-kerberos".equals(appName)) {
                return USER_KERBEROS_CONF;
            } else if ("hadoop-keytab-kerberos".equals(appName)) {
                if (PlatformName.IBM_JAVA) {
                    KEYTAB_KERBEROS_OPTIONS.put("useKeytab", prependFileAuthority(keytabFile));
                } else {
                    KEYTAB_KERBEROS_OPTIONS.put("keyTab", keytabFile);
                }

                KEYTAB_KERBEROS_OPTIONS.put("principal", keytabPrincipal);
                return KEYTAB_KERBEROS_CONF;
            } else {
                return null;
            }
        }
    }
}
