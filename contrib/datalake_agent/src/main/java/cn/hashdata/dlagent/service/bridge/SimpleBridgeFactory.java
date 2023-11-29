package cn.hashdata.dlagent.service.bridge;

import cn.hashdata.dlagent.api.model.RequestContext;
import cn.hashdata.dlagent.service.utilities.BasePluginFactory;
import cn.hashdata.dlagent.service.utilities.GSSFailureHandler;
import org.springframework.stereotype.Component;

@Component
public class SimpleBridgeFactory implements BridgeFactory {

    private final BasePluginFactory pluginFactory;
    private final GSSFailureHandler failureHandler;

    public SimpleBridgeFactory(BasePluginFactory pluginFactory, GSSFailureHandler failureHandler) {
        this.pluginFactory = pluginFactory;
        this.failureHandler = failureHandler;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Bridge getBridge(RequestContext context) {
        return new ReadBridge(pluginFactory, context, failureHandler);
    }
}
