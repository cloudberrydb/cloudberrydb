package cn.hashdata.dlagent.service.bridge;

import cn.hashdata.dlagent.api.model.RequestContext;

/**
 * Factory for creating instances of Bridge interfaces based on request context
 */
public interface BridgeFactory {

    /**
     * Returns an instance of the Bridge for reading or writing data based on the request context
     *
     * @param context       request context
     * @return an instance of the Bridge suitable for a given request
     */
    Bridge getBridge(RequestContext context);
}
