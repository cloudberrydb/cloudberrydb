package cn.hashdata.dlagent.service.spring;

import cn.hashdata.dlagent.api.error.DlRuntimeException;
import org.springframework.core.task.TaskRejectedException;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;

/**
 * A {@link ThreadPoolTaskExecutor} that enhances error reporting when a
 * {@link TaskRejectedException} occurs. The error messages provide hints on
 * how to overcome {@link TaskRejectedException} errors, by suggesting tuning
 * parameters for dlagent.
 */
public class DlThreadPoolTaskExecutor extends ThreadPoolTaskExecutor {

    private static final String DLAGENT_SERVER_PROCESSING_CAPACITY_EXCEEDED_MESSAGE = "dlagent Server processing capacity exceeded.";
    private static final String DLAGENT_SERVER_PROCESSING_CAPACITY_EXCEEDED_HINT = "Consider increasing the values of 'dlagent.task.pool.max-size' and/or 'dlagent.task.pool.queue-capacity' in 'dlagent-application.properties'";

    /**
     * Submits a {@link Runnable} to the executor. Handles
     * {@link TaskRejectedException} errors by enhancing error reporting.
     *
     * @param task the {@code Runnable} to execute (never {@code null})
     * @return a Future representing pending completion of the task
     * @throws TaskRejectedException if the given task was not accepted
     */
    @Override
    public Future<?> submit(Runnable task) {
        try {
            return super.submit(task);
        } catch (TaskRejectedException ex) {
            DlRuntimeException exception = new DlRuntimeException(
                    DLAGENT_SERVER_PROCESSING_CAPACITY_EXCEEDED_MESSAGE,
                    String.format(DLAGENT_SERVER_PROCESSING_CAPACITY_EXCEEDED_HINT),
                    ex.getCause());
            throw new TaskRejectedException(ex.getMessage(), exception);
        }
    }

    /**
     * Submits a {@link Runnable} to the executor. Handles
     * {@link TaskRejectedException} errors by enhancing error reporting.
     *
     * @param task the {@code Callable} to execute (never {@code null})
     * @return a Future representing pending completion of the task
     * @throws TaskRejectedException if the given task was not accepted
     */
    @Override
    public <T> Future<T> submit(Callable<T> task) {
        try {
            return super.submit(task);
        } catch (TaskRejectedException ex) {
            DlRuntimeException exception = new DlRuntimeException(
                    DLAGENT_SERVER_PROCESSING_CAPACITY_EXCEEDED_MESSAGE,
                    String.format(DLAGENT_SERVER_PROCESSING_CAPACITY_EXCEEDED_HINT),
                    ex.getCause());
            throw new TaskRejectedException(ex.getMessage(), exception);
        }
    }
}
