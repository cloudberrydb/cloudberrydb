package cn.hashdata.dlagent.service.bridge;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import cn.hashdata.dlagent.api.io.Writable;
import cn.hashdata.dlagent.api.model.RequestContext;
import cn.hashdata.dlagent.service.utilities.BasePluginFactory;
import cn.hashdata.dlagent.service.utilities.GSSFailureHandler;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import cn.hashdata.dlagent.api.io.BufferWritable;

import java.nio.charset.StandardCharsets;

/**
 * ReadBridge class creates appropriate accessor and resolver. It will then
 * create the correct output conversion class (e.g. Text or GPDBWritable) and
 * get records from accessor, let resolver deserialize them and serialize them
 * again using the output conversion class. <br>
 * The class handles BadRecordException and other exception type and marks the
 * record as invalid for GPDB.
 */
public class ReadBridge extends BaseBridge {
    private static final String DEFAULT_RESPONSE = "{}";

    public ReadBridge(BasePluginFactory pluginFactory, RequestContext context, GSSFailureHandler failureHandler) {
        super(pluginFactory, context, failureHandler);
    }

    /**
     * Accesses the underlying data source.
     */
    @Override
    public boolean open() throws Exception {
        // using lambda and not a method reference accessor::openForRead as the accessor will be changed by the retry function
        return failureHandler.execute(context.getConfiguration(), "open", () -> accessor.open(), this::beforeRetryCallback);
    }

    protected Writable makeOutput(Object value) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(MapperFeature.USE_ANNOTATIONS, true); // enable annotations for serialization
        mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY); // ignore empty fields

        if (value == null) {
            byte[] output = DEFAULT_RESPONSE.getBytes(StandardCharsets.UTF_8);
            return new BufferWritable(output, output.length);
        }

        byte[] output = mapper.writeValueAsBytes(value);
        return new BufferWritable(output, output.length);
    }

    @Override
    public Writable getFragments(String pattern) throws Exception {
        // we checked before that outputQueue is empty, so we can override it.
        return makeOutput(accessor.getFragments(pattern));
    }

    @Override
    public Writable getPartitions(String pattern) throws Exception {
        // we checked before that outputQueue is empty, so we can override it.
        return makeOutput(accessor.getPartitions(pattern));
    }

    @Override
    public Writable getSchema(String pattern) throws Exception {
        // we checked before that outputQueue is empty, so we can override it.
        return makeOutput(accessor.getSchema(pattern));
    }

    /**
     * Close the underlying resource
     */
    public void close() throws Exception {
        try {
            accessor.close();
        } catch (Exception e) {
            LOG.error("Failed to close bridge resources: {}", e.getMessage());
            throw e;
        }
    }
}
