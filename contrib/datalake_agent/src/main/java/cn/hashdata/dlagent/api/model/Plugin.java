package cn.hashdata.dlagent.api.model;

/**
 * Base interface for all plugin types
 */
public interface Plugin {

    /**
     * Sets the context for the current request
     *
     * @param context the context for the current request
     */
    void setRequestContext(RequestContext context);

    /**
     * Invoked after the {@code RequestContext} has been bound
     */
    void afterPropertiesSet();
}
