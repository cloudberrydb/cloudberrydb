package cn.hashdata.dlagent.service.controller;

import cn.hashdata.dlagent.api.model.RequestContext;

import java.io.OutputStream;

/**
 * Service that reads data from external systems.
 */
public interface ReadService {

    /**
     * Reads data from the external system specified by the RequestContext.
     * The data is then written to the provided OutputStream.
     *
     * @param context      request context
     * @param outputStream output stream to write data to
     */
    void readData(RequestContext context, OutputStream outputStream);
}
