package cn.hashdata.dlagent.service.spring;

import cn.hashdata.dlagent.api.configuration.DlServerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.task.TaskExecutionProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.task.TaskExecutorBuilder;
import org.springframework.boot.task.TaskExecutorCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.TaskDecorator;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.annotation.AsyncAnnotationBeanPostProcessor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.web.servlet.config.annotation.AsyncSupportConfigurer;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * Configures the {@link AsyncTaskExecutor} for tasks that will stream data to
 * clients
 */
@Configuration
@EnableConfigurationProperties(DlServerProperties.class)
public class DlConfiguration implements WebMvcConfigurer {

    /**
     * Bean name of dlagent's {@link TaskExecutor}.
     */
    public static final String DLAGENT_RESPONSE_STREAM_TASK_EXECUTOR = "dlagentResponseStreamTaskExecutor";
    private static final Logger LOG = LoggerFactory.getLogger(DlConfiguration.class);

    private final ListableBeanFactory beanFactory;

    /**
     * Constructs a dlagent Configuration object with the provided
     * {@link ListableBeanFactory}
     *
     * @param beanFactory the beanFactory
     */
    public DlConfiguration(ListableBeanFactory beanFactory) {
        this.beanFactory = beanFactory;
    }

    /**
     * Configures the TaskExecutor to be used for async requests (i.e. Bridge
     * Read).
     */
    @Override
    public void configureAsyncSupport(AsyncSupportConfigurer configurer) {
        AsyncTaskExecutor taskExecutor = (AsyncTaskExecutor) this.beanFactory
                .getBean(DLAGENT_RESPONSE_STREAM_TASK_EXECUTOR);
        configurer.setTaskExecutor(taskExecutor);
    }

    /**
     * Configures and builds the {@link ThreadPoolTaskExecutor}
     *
     * @return the {@link ThreadPoolTaskExecutor}
     */
    @Bean(name = {DLAGENT_RESPONSE_STREAM_TASK_EXECUTOR,
            AsyncAnnotationBeanPostProcessor.DEFAULT_TASK_EXECUTOR_BEAN_NAME})
    public ThreadPoolTaskExecutor dlagentApplicationTaskExecutor(DlServerProperties dlagentServerProperties,
                                                                 ObjectProvider<TaskExecutorCustomizer> taskExecutorCustomizers,
                                                                 ObjectProvider<TaskDecorator> taskDecorator) {

        TaskExecutionProperties properties = dlagentServerProperties.getTask();
        TaskExecutionProperties.Pool pool = properties.getPool();
        TaskExecutorBuilder builder = new TaskExecutorBuilder();
        builder = builder.queueCapacity(pool.getQueueCapacity());
        builder = builder.corePoolSize(pool.getCoreSize());
        builder = builder.maxPoolSize(pool.getMaxSize());
        builder = builder.allowCoreThreadTimeOut(pool.isAllowCoreThreadTimeout());
        builder = builder.keepAlive(pool.getKeepAlive());
        TaskExecutionProperties.Shutdown shutdown = properties.getShutdown();
        builder = builder.awaitTermination(shutdown.isAwaitTermination());
        builder = builder.awaitTerminationPeriod(shutdown.getAwaitTerminationPeriod());
        builder = builder.threadNamePrefix(properties.getThreadNamePrefix());
        builder = builder.customizers(taskExecutorCustomizers.orderedStream()::iterator);
        builder = builder.taskDecorator(taskDecorator.getIfUnique());

        LOG.debug("Initializing dlagent ThreadPoolTaskExecutor with prefix={}. " +
                        "Pool options: " +
                        "queue capacity={}, core size={}, max size={}, " +
                        "allow core thread timeout={}, keep alive={}. " +
                        "Shutdown options: await termination={}, await " +
                        "termination period={}.",
                properties.getThreadNamePrefix(),
                pool.getQueueCapacity(),
                pool.getCoreSize(),
                pool.getMaxSize(),
                pool.isAllowCoreThreadTimeout(),
                pool.getKeepAlive(),
                shutdown.isAwaitTermination(),
                shutdown.getAwaitTerminationPeriod());

        return builder.build(DlThreadPoolTaskExecutor.class);
    }

}
