package cn.hashdata.dlagent.service.spring;

import org.slf4j.MDC;
import org.springframework.core.task.TaskDecorator;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * A task decorator that gets a copy of the MDC context map in the web thread
 * context, and then sets the context map to the async thread context.
 */
@Component
public class DlContextMdcLogEnhancerDecorator implements TaskDecorator {

    /**
     * {@inheritDoc}
     */
    @Override
    public Runnable decorate(Runnable runnable) {
        // Web thread context: (Grab the current thread MDC data)
        Map<String, String> contextMap = MDC.getCopyOfContextMap();
        return () -> {
            try {
                // @Async thread context: (Restore the Web thread context's MDC data)
                MDC.setContextMap(contextMap);
                runnable.run();
            } finally {
                MDC.clear();
            }
        };
    }
}
