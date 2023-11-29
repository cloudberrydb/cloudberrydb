package cn.hashdata.dlagent.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import cn.hashdata.dlagent.api.model.RequestContext;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Service responsible for submitting metrics to MeterRegistry.
 */
@Component
@Slf4j
public class MetricsReporter {

    private static final String UNKNOWN_VALUE = "unknown";
    private static final Tags SUCCESS_TAG = Tags.of("outcome", "success");
    private static final Tags ERROR_TAG = Tags.of("outcome", "error");

    private final MeterRegistry registry;
    private final Environment env;

    /**
     * Creates a new instance.
     *
     * @param registry meter registry to submit metrics to
     * @param env      environment which serves as the source of property values
     */
    public MetricsReporter(MeterRegistry registry, Environment env) {
        this.registry = registry;
        this.env = env;
    }

    /**
     * Reports timer metric with a given name and duration to the registry. Applies custom tags before reporting.
     *
     * @param metric   metric to apply
     * @param duration duration measured by the metric
     * @param context  request context
     */
    public void reportTimer(DlAgentMetric metric, Duration duration, RequestContext context) {
        reportTimer(metric, duration, context, null);
    }

    /**
     * Reports timer metric, duration and outcome of the timed operation to the registry.
     * Applies custom tags before reporting and adds an outcome tag.
     *
     * @param metric   metric to apply
     * @param duration duration measured by the metric
     * @param context  request context
     * @param success  true if timed operation was successful, false otherwise
     */
    public void reportTimer(DlAgentMetric metric, Duration duration, RequestContext context, boolean success) {
        reportTimer(metric, duration, context, success ? SUCCESS_TAG : ERROR_TAG);
    }

    /**
     * Reports timer metric with a given name, duration and additional tags to the registry.
     * Applies custom tags before reporting and adds a success outcome tag.
     *
     * @param metric
     * @param duration
     * @param context
     * @param extraTags
     */
    private void reportTimer(DlAgentMetric metric, Duration duration, RequestContext context, Tags extraTags) {
        String metricName = metric.getMetricName();
        long durationMs = duration.toMillis();
        if (!env.getProperty(metric.getEnabledPropertyName(), Boolean.class, Boolean.FALSE)) {
            log.trace("Skipping reporting metric {} with duration={}ms", metricName, durationMs);
            return;
        }

        Tags tags = (extraTags == null) ? getTags(context) : getTags(context).and(extraTags);
        try {
            Timer timer = Timer.builder(metricName).tags(tags).register(registry);
            timer.record(duration);
            if (log.isTraceEnabled()) {
                log.trace("Reported timer {}{} with duration={}ms", metricName, tags, durationMs);
            }
        } catch (Exception e) {
            log.warn(String.format("Unable to report timer %s%s with duration=%dms.", metricName, tags, durationMs), e);
        }
    }

    /**
     * Reports counter metric with a given name and the increment to the registry.
     * Reports with any tags given by the context and adds a success outcome tag.
     *
     * @param metric
     * @param increment
     * @param context
     */
    public void reportCounter(DlAgentMetric metric, long increment, RequestContext context) {
        String metricName = metric.getMetricName();
        if (!env.getProperty(metric.getEnabledPropertyName(), Boolean.class, Boolean.FALSE)) {
            log.trace("Skipping reporting metric {} with increment={}", metricName, increment);
            return;
        }
        Tags tags = getTags(context);
        try {
            Double incrementCount = Long.valueOf(increment).doubleValue();
            Counter counter = Counter.builder(metricName).tags(tags).register(registry);
            counter.increment(incrementCount);
            if (log.isTraceEnabled()) {
                log.trace("Reported counter {}{} with increment={}", metricName, tags, increment);
            }
        } catch (Exception e) {
            log.warn(String.format("Unable to report counter %s%s with increment=%d.", metricName, tags, increment), e);
        }
    }

    /**
     * Pulls the value for reporting frequency for the given metric from the environment.
     * If no value found, the default reporting frequency is 1000.
     */
    public long getReportFrequency() {
        long reportFrequency = env.getProperty("dlagent.metrics.report-frequency", Long.class, 1000L);
        if (reportFrequency < 0) {
            log.warn("Tried to set reporting frequency to {}. We do not support negative numbers, setting to 0.", reportFrequency);
            reportFrequency = 0;
        }
        return reportFrequency;
    }

    /**
     * Produces a set of custom tags with values from the provided request context.
     *
     * @param context request context
     * @return collection of custom tags
     */
    private Tags getTags(RequestContext context) {
        return Tags.empty()
                .and("user", StringUtils.defaultIfBlank(context.getUser(), UNKNOWN_VALUE))
                .and("profile", StringUtils.defaultIfBlank(context.getProfile(), UNKNOWN_VALUE))
                .and("server", StringUtils.defaultIfBlank(context.getServerName(), "default"));
    }

    /**
     * Enum that has information about all custom metrics for dlagent.
     */
    public enum DlAgentMetric {
        PARTITION_GET("dlagent.partition.get", "dlagent.metrics.partition.enabled"),
        METADATA_GET("dlagent.metadata.get", "dlagent.metrics.metadata.enabled"),
        FRAGMENT_GET("dlagent.fragment.get", "dlagent.metrics.fragment.enabled");

        private final String metricName;
        private final String enabledPropertyName;

        /**
         * Creates a new instance.
         *
         * @param metricName          name of the metric as will be seen by the registry
         * @param enabledPropertyName property name that manages whether reporting of this metric is turned on / off
         */
        DlAgentMetric(String metricName, String enabledPropertyName) {
            this.metricName = metricName;
            this.enabledPropertyName = enabledPropertyName;
        }

        public String getMetricName() {
            return metricName;
        }

        public String getEnabledPropertyName() {
            return enabledPropertyName;
        }
    }
}
