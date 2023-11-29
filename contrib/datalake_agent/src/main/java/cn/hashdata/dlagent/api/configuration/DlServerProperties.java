package cn.hashdata.dlagent.api.configuration;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.autoconfigure.task.TaskExecutionProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Configuration properties for dlagent.
 */
@ConfigurationProperties(prefix = DlServerProperties.PROPERTY_PREFIX)
public class DlServerProperties {
    /**
     * The property prefix for all properties in this group.
     */
    public static final String PROPERTY_PREFIX = "dlagent";

    /**
     * Customizable settings for tomcat through dlagent
     */
    @Getter
    @Setter
    private Tomcat tomcat = new Tomcat();

    /**
     * Configurable task execution properties for async tasks (i.e Bridge Read)
     */
    @Getter
    @Setter
    private TaskExecutionProperties task = new TaskExecutionProperties();

    @Getter
    @Setter
    public static class Tomcat {

        /**
         * Maximum number of headers allowed in the request
         */
        private int maxHeaderCount = 30000;

        /**
         * Whether upload requests will use the same read timeout as connectionTimeout
         */
        private boolean disableUploadTimeout = true; // default Tomcat setting

        /**
         * Timeout for reading data from upload requests, if disableUploadTimeout is set to false.
         */
        private Duration connectionUploadTimeout = Duration.ofMinutes(5); // 5 min is default Tomcat setting

    }
}
