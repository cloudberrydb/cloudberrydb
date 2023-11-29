package cn.hashdata.dlagent.api.model;

import org.apache.hadoop.conf.Configuration;

import java.util.Map;

public interface ConfigurationFactory {
    /**
     * Configuration property that stores the server name
     */
    String DLAGENT_SERVER_NAME_PROPERTY = "dlagent.config.server.name";

    /**
     * Synthetic configuration property that stores the user so that is can be
     * used in config files for interpolation in other properties, for example
     * in JDBC when setting session authorization from a proxy user to the
     * end-user
     */
    String DLAGENT_SESSION_USER_PROPERTY = "dlagent.session.user";

    /**
     * Initializes a configuration object that applies server-specific configurations and
     * adds additional properties on top of it, if specified.
     *
     * @param configFiles name of the configuration
     * @param serverName name of the server
     * @param userName name of the user
     * @return configuration object
     */
    Configuration initConfiguration(String catalogType,
                                    String configFiles,
                                    String serverName,
                                    String userName);
}
