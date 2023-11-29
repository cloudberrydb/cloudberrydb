package cn.hashdata.dlagent.service.controller;

import com.google.common.io.CountingOutputStream;
import lombok.extern.slf4j.Slf4j;
import cn.hashdata.dlagent.api.model.ConfigurationFactory;
import cn.hashdata.dlagent.api.model.RequestContext;
import cn.hashdata.dlagent.service.MetricsReporter;
import cn.hashdata.dlagent.service.bridge.Bridge;
import cn.hashdata.dlagent.service.bridge.BridgeFactory;
import cn.hashdata.dlagent.service.security.SecurityService;
import org.springframework.stereotype.Service;

import java.io.DataOutputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.time.Instant;

/**
 * Implementation of the ReadService.
 */
@Service
@Slf4j
public class ReadServiceImpl extends BaseServiceImpl<OperationStats> implements ReadService {
    /**
     * Creates a new instance.
     *
     * @param configurationFactory configuration factory
     * @param bridgeFactory        bridge factory
     * @param securityService      security service
     * @param metricsReporter      metrics reporter service
     */
    public ReadServiceImpl(ConfigurationFactory configurationFactory,
                           BridgeFactory bridgeFactory,
                           SecurityService securityService,
                           MetricsReporter metricsReporter) {
        super("Read", configurationFactory, bridgeFactory, securityService, metricsReporter);
    }

    @Override
    public void readData(RequestContext context, OutputStream outputStream) {
        // wrapping the invocation of processData(..) with the error reporting logic
        // since any exception thrown from it must be logged, as this method is called asynchronously
        // and is the last opportunity to log the exception while having MDC logging context defined
        invokeWithErrorHandling(() -> processData(context, () -> writeStream(context, outputStream)));
    }

    private OperationResult writeStream(RequestContext context, OutputStream outputStream) {
        OperationStats queryStats = new OperationStats(metricsReporter, context);
        OperationResult queryResult = new OperationResult();

        // dataStream (and outputStream as the result) will close automatically at the end of the try block
        CountingOutputStream countingOutputStream = new CountingOutputStream(outputStream);
        String sourceName = context.getDataSource();
        try {
            processRequest(countingOutputStream, context, queryStats);
        } catch (Exception e) {
            // the exception is not re-thrown but passed to the caller in the queryResult so that
            // the caller has a chance to inspect / report query stats before re-throwing the exception
            queryResult.setException(e);
            queryResult.setSourceName(sourceName);
        } finally {
            queryResult.setStats(queryStats);
        }

        return queryResult;
    }

    private void processRequest(CountingOutputStream countingOutputStream,
                                RequestContext context,
                                OperationStats queryStats) throws Exception {
        DataOutputStream dos = new DataOutputStream(countingOutputStream);
        boolean success = false;
        Instant startTime = Instant.now();
        Bridge bridge = null;

        try {
            bridge = getBridge(context);
            log.debug("Starting processing request: methodName {} resource {} tableName {}",
                    context.getMethod(), context.getDataSource(), context.getTableName());
            bridge.open();

            call(bridge, context, dos, queryStats);
            success = true;
        } finally {
            if (bridge != null) {
                try {
                    bridge.close();
                } catch (Exception e) {
                    log.warn("Ignoring error encountered during bridge.close()", e);
                }
            }

            Duration duration = Duration.between(startTime, Instant.now());
            queryStats.reportCompletedRpcCount();
            log.debug("Finished processing request: methodName {} resource {} tableName in {} ms.",
                    context.getMethod(), context.getDataSource(), duration.toMillis());
            metricsReporter.reportTimer(queryStats.getOperation().getMetric(), duration, context, success);
        }
    }

    private void call(Bridge bridge, RequestContext context, DataOutputStream dos, OperationStats queryStats) throws Exception{
        switch (context.getMethod()) {
            case "getPartitions":
                queryStats.setOperation(OperationStats.Operation.PARTITION_GET);
                bridge.getPartitions(context.getTableName()).write(dos);
                break;
            case "getFragments":
                queryStats.setOperation(OperationStats.Operation.FRAGMENT_GET);
                bridge.getFragments(context.getTableName()).write(dos);
                break;
            case "getSchema":
                queryStats.setOperation(OperationStats.Operation.METADATA_GET);
                bridge.getSchema(context.getTableName()).write(dos);
                break;
            default:
                throw new UnsupportedOperationException("unknown method:\""+ context.getMethod() + "\".");
        }
    }
}
