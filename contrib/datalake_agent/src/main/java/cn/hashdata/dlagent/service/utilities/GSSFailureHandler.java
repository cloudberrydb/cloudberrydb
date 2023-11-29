package cn.hashdata.dlagent.service.utilities;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import cn.hashdata.dlagent.api.utilities.Utilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.Callable;

/**
 * A class that executes an operation and retries it upto a maximum allowed number of times
 * in case the operation throws a specific GSS exception.
 */
@Component
public class GSSFailureHandler {

    private static final Logger LOG = LoggerFactory.getLogger(GSSFailureHandler.class);
    private static final String RETRIES_PROPERTY_NAME = "dlagent.sasl.connection.retries";
    private static final int MAX_RETRIES_DEFAULT = 5;
    private static final String ERROR_MESSAGE_PATTERN = "GSS initiate failed";
    private static final int MAX_BACKOFF = 1000; // max backoff in ms between attempts

    /**
     * Executes a provided operation and retries it upto a maximum allowed number of times if the operation throws
     * "GSS initiate failed" exception. If the provided configuration indicates that the Hadoop cluster is not
     * secured by Kerberos, the operation is executed only once.
     *
     * @param configuration current configuration
     * @param operationName name of the operation (printed in the logs)
     * @param operation     operation to execute
     * @param <T>           type of return value
     * @return the return value of the operation
     * @throws Exception if an exception occurs during the operation and can not be overcome with retries
     */
    public <T> T execute(Configuration configuration, String operationName, Callable<T> operation, Runnable callback) throws Exception {
        /*
         max attempts to execute an operation is 1 for an unsecured cluster
         if the cluster is secured with Kerberos and the same principal is used for all hosts and a lot of queries
         arrive at the same time, a Namenode can report a "GSS initiate failed (Request is a replay (34))" error
         (note that with Hadoop client 2.9.2 "Request is a replay (34)" message is only available in Namenode logs.
         To overcome the error, we will retry the operation a few times.
         */

        boolean securityEnabled = Utilities.isSecurityEnabled(configuration);
        int configuredRetries = securityEnabled ? configuration.getInt(RETRIES_PROPERTY_NAME, MAX_RETRIES_DEFAULT) : 0;
        if (configuredRetries < 0) {
            throw new RuntimeException(String.format("Property %s can not be set to a negative value %d",
                    RETRIES_PROPERTY_NAME, configuredRetries));
        }

        // will attempt to execute the logic at least once
        int maxAttempts = configuredRetries + 1;
        LOG.debug("Before [{}] operation, security = {}, max attempts = {}", operationName, securityEnabled, maxAttempts);

        // retry upto max allowed number of attempts
        T result = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                LOG.debug("Operation [{}] attempt #{} of {}", operationName, attempt, maxAttempts);
                result = operation.call();
                break;
            } catch (IOException e) {
                // non-secure cluster or other IOException needs to be rethrown
                if (!securityEnabled || !StringUtils.contains(e.getMessage(), ERROR_MESSAGE_PATTERN)) {
                    throw e;
                }
                LOG.warn(String.format("Attempt #%d of %d failed to %s: ", attempt, maxAttempts, operationName), e.getMessage());

                if (attempt >= maxAttempts) {
                    // reached the max allowed number of attempts, no more retries allowed
                    throw e;
                }

                // still have an attempt to retry the operation, backoff by pausing the thread
                int backoffMs = new Random().nextInt(MAX_BACKOFF) + 1;
                LOG.debug("Backing off for {} ms before the next attempt", backoffMs);
                try {
                    Thread.sleep(backoffMs);
                } catch (InterruptedException ie) {
                    // if interrupted, we should not be trying another attempts, just rethrow the original exception
                    LOG.warn("Interrupted while sleeping for backoff of [{}] operation", operationName);
                    throw e;
                }
                if (callback != null) {
                    // execute a callback before next retry attempt
                    LOG.debug("Calling callback function before the next attempt");
                    callback.run();
                }
            }
        }
        return result;
    }

    public <T> T execute(Configuration configuration, String operationName, Callable<T> operation) throws Exception {
        return execute(configuration, operationName, operation, null);
    }

}
