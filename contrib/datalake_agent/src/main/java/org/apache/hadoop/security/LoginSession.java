package org.apache.hadoop.security;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.hadoop.security.User;
import org.apache.hadoop.security.UserGroupInformation;

import javax.security.auth.Subject;
import java.util.Objects;

/**
 * This class stores information about Kerberos login details for a given configuration server.
 * A subset of fields establishes session identity based on the server configuration.
 * Other fields are the result of the login action and do not establish identity.
 * <p>
 * This class has to be a member of <code>org.apache.hadoop.security</code> package as it needs access
 * to package-private <code>User</code> class.
 */
public class LoginSession {

    // fields establishing session identity from configuration
    private String configDirectory;
    private String principalName;
    private String keytabPath;
    private long kerberosMinMillisBeforeRelogin;

    // derived fields stored to be re-used for subsequent requests
    private UserGroupInformation loginUser;
    private Subject subject;
    private User user;

    /**
     * Creates a new session object with a config directory
     *
     * @param configDirectory server configuration directory
     */
    public LoginSession(String configDirectory) {
        this(configDirectory, null, null, null, null, 0L);
    }

    /**
     * Creates a new session object with a config directory and a login user
     *
     * @param configDirectory server configuration directory
     * @param loginUser       UserGroupInformation for the given principal after login to Kerberos was performed
     */
    public LoginSession(String configDirectory, UserGroupInformation loginUser) {
        this(configDirectory, null, null, loginUser, null, 0L);
    }

    /**
     * Creates a new session object.
     *
     * @param configDirectory server configuration directory
     * @param principalName   Kerberos principal name to use to obtain tokens
     * @param keytabPath      full path to a keytab file for the principal
     */
    public LoginSession(String configDirectory, String principalName, String keytabPath, long kerberosMinMillisBeforeRelogin) {
        this(configDirectory, principalName, keytabPath, null, null, kerberosMinMillisBeforeRelogin);
    }

    /**
     * Creates a new session object.
     *
     * @param configDirectory                server configuration directory
     * @param principalName                  Kerberos principal name to use to obtain tokens
     * @param keytabPath                     full path to a keytab file for the principal
     * @param loginUser                      UserGroupInformation for the given principal after login to Kerberos was performed
     * @param subject                        the subject
     * @param kerberosMinMillisBeforeRelogin the number of milliseconds before re-login
     */
    public LoginSession(String configDirectory, String principalName, String keytabPath, UserGroupInformation loginUser,
                        Subject subject, long kerberosMinMillisBeforeRelogin) {
        this.configDirectory = configDirectory;
        this.principalName = principalName;
        this.keytabPath = keytabPath;
        this.loginUser = loginUser;
        this.subject = subject;
        if (subject != null) {
            this.user = subject.getPrincipals(User.class).iterator().next();
        }
        this.kerberosMinMillisBeforeRelogin = kerberosMinMillisBeforeRelogin;
    }

    /**
     * Get the login UGI for this session.
     *
     * @return the UGI for this session
     */
    public UserGroupInformation getLoginUser() {
        return loginUser;
    }

    /**
     * Get the minimal number of milliseconds before re-login.
     *
     * @return the minimal number of milliseconds before re-login.
     */
    public long getKerberosMinMillisBeforeRelogin() {
        return kerberosMinMillisBeforeRelogin;
    }

    /**
     * Get the Subject used for this session.
     *
     * @return the Subject for this session
     */
    public Subject getSubject() {
        return subject;
    }

    /**
     * Get the User for this session.
     *
     * @return the User for this session.
     */
    public User getUser() {
        return user;
    }

    /**
     * Get the path of the keytab file used to login.
     *
     * @return the path of the keytab file used to login
     */
    public String getKeytabPath() {
        return keytabPath;
    }

    /**
     * Get the name of the Kerberos principal used to login.
     *
     * @return the name of the Kerberos principal used to login
     */
    public String getPrincipalName() {
        return principalName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LoginSession that = (LoginSession) o;
        // ugi and subject are not included into expression below as they are transient derived values
        return Objects.equals(configDirectory, that.configDirectory) &&
                Objects.equals(principalName, that.principalName) &&
                Objects.equals(keytabPath, that.keytabPath) &&
                kerberosMinMillisBeforeRelogin == that.kerberosMinMillisBeforeRelogin;
    }

    @Override
    public int hashCode() {
        return Objects.hash(configDirectory, principalName, keytabPath, kerberosMinMillisBeforeRelogin);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("config", configDirectory)
                .append("principal", principalName)
                .append("keytab", keytabPath)
                .append("kerberosMinMillisBeforeRelogin", kerberosMinMillisBeforeRelogin)
                .toString();
    }
}
