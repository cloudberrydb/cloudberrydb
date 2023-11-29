package cn.hashdata.dlagent.service.bridge;

import cn.hashdata.dlagent.api.model.MetadataFetcher;
import cn.hashdata.dlagent.api.model.RequestContext;
import cn.hashdata.dlagent.service.utilities.BasePluginFactory;
import cn.hashdata.dlagent.service.utilities.GSSFailureHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract class representing the bridge that provides to subclasses logger and accessor and
 * resolver instances obtained from the factories.
 */
public abstract class BaseBridge implements Bridge {

    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    protected MetadataFetcher accessor;
    protected BasePluginFactory pluginFactory;
    protected RequestContext context;
    protected GSSFailureHandler failureHandler;

    /**
     * Creates a new instance of the bridge.
     *
     * @param pluginFactory plugin factory
     * @param context request context
     * @param failureHandler failure handler
     */
    public BaseBridge(BasePluginFactory pluginFactory, RequestContext context, GSSFailureHandler failureHandler) {
        this.pluginFactory = pluginFactory;
        this.context = context;
        this.failureHandler = failureHandler;

        String accessorClassName = context.getMetadataFetcher();
        LOG.debug("Creating accessor '{}'", accessorClassName);

        this.accessor = pluginFactory.getPlugin(context, accessorClassName);
    }

    /**
     * A function that is called by the failure handler before a new retry attempt after a failure.
     * It re-creates the accessor from the factory in case the accessor implementation is not idempotent.
     */
    protected void beforeRetryCallback() {
        String accessorClassName = context.getMetadataFetcher();
        LOG.debug("Creating accessor '{}'", accessorClassName);
        this.accessor = pluginFactory.getPlugin(context, accessorClassName);
    }
}
