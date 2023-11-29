package cn.hashdata.dlagent.service.controller;

import cn.hashdata.dlagent.api.error.DlRuntimeException;
import lombok.Getter;
import lombok.Setter;
import cn.hashdata.dlagent.api.model.RequestContext;
import cn.hashdata.dlagent.service.MetricsReporter;

/**
 * Holds statistics about performed operation.
 */
public class OperationStats {
    @Getter
    private final RequestContext context;
    private final MetricsReporter metricsReporter;
    private final long reportFrequency;
    @Getter
    private long rpcCount = 0;
    @Getter
    @Setter
    private long lastReportedRpcCount = 0;
    @Getter
    @Setter
    private Operation operation;

    enum Operation {
        PARTITION_GET(MetricsReporter.DlAgentMetric.PARTITION_GET),
        METADATA_GET(MetricsReporter.DlAgentMetric.METADATA_GET),
        FRAGMENT_GET(MetricsReporter.DlAgentMetric.FRAGMENT_GET);

        private final MetricsReporter.DlAgentMetric rpcCountMetric;

        Operation(MetricsReporter.DlAgentMetric recordMetric) {
            this.rpcCountMetric = recordMetric;
        }

        public MetricsReporter.DlAgentMetric getMetric() {
            return rpcCountMetric;
        }
    }

    public OperationStats(MetricsReporter metricsReporter, RequestContext context) {
        this.context = context;
        this.metricsReporter = metricsReporter;
        this.reportFrequency = metricsReporter.getReportFrequency();
    }

    /**
     * Add a completed record to the operation's stats. Report the stats when necessary.
     *
     */
    public void reportCompletedRpcCount() {
        rpcCount++;

        if ((reportFrequency != 0) && (rpcCount % reportFrequency == 0)) {
            flushStats();
        }
    }

    /**
     * Send all the stats to the metric reporter. Set last reported values.
     */
    public void flushStats() {
        if (reportFrequency == 0) {
            return;
        }

        long rpcProcessed = rpcCount - lastReportedRpcCount;
        if (rpcProcessed != 0) {
            metricsReporter.reportCounter(operation.rpcCountMetric, rpcProcessed, context);
            lastReportedRpcCount = rpcCount;
        }
    }
}
