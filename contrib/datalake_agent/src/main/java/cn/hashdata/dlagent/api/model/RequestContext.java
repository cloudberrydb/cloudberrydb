package cn.hashdata.dlagent.api.model;

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

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import cn.hashdata.dlagent.api.utilities.ColumnDescriptor;
import cn.hashdata.dlagent.api.utilities.Utilities;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Optional;

/**
 * Common configuration available to all dlagent plugins. Represents input data
 * coming from client applications, such as GPDB.
 */
@Getter
@Setter
public class RequestContext {
    /**
     * The name of the server configuration for this request.
     */
    private String config;

    /**
     * The server configuration associated to the server that this request is
     * accessing
     */
    private Configuration configuration;

    /**
     * The data source of the required resource (i.e a file path or a table
     * name).
     */
    private String dataSource;


    private String metadataFetcher;

    /**
     * The Greenplum command count
     */
    private int gpCommandCount;

    /**
     * The Greenplum session ID
     */
    private int gpSessionId;

    /**
     * The plugin configuration
     */
    private PluginConf pluginConf;

    /**
     * The name of the profile associated to this request.
     */
    private String profile;

    /**
     * The name of the method associated to this request.
     */
    private String method;

    /**
     * The name of the origin Greenplum schema name.
     */
    private String schemaName;

    /**
     * The name of the origin Greenplum table name.
     */
    private String tableName;

    /**
     * The transaction ID for the current Greenplum query.
     */
    private String transactionId;

    /**
     * The name of the server to access.
     */
    @Setter(AccessLevel.NONE)
    private String serverName = "default";

    /**
     * The filter string, <tt>null</tt> if #hasFilter is <tt>false</tt>.
     */
    private String filterString;

    /**
     * The number of segments in Greenplum.
     */
    private int totalSegments;

    /**
     * The list of column descriptors
     */
    private List<ColumnDescriptor> tupleDescription = new ArrayList<>();

    /**
     * The identity of the end-user making the request.
     */
    private String user;

    private String catalogType;

    private String path;

    /**
     * Number of attributes projected in query.
     * <p>
     * Example:
     * SELECT col1, col2, col3... : number of attributes projected - 3
     * SELECT col1, col2, col3... WHERE col4=a : number of attributes projected - 4
     * SELECT *... : number of attributes projected - 0
     */
    private int numAttrsProjected;

    /**
     * The split size(mb) for this scan
     */
    private int splitSize;

    /**
     * The query type of hudi table
     */
    private String queryType;

    /**
     * Enable hudi MetaTable Table
     */
    private String metadataTableEnabled;

    private String defaultPartitionName;

    public String getDefaultPartitionName() {
        return "__HIVE_DEFAULT_PARTITION__";
    }

    /**
     * Returns true if there is a filter string to parse.
     *
     * @return whether there is a filter string
     */
    public boolean hasFilter() {
        return filterString != null;
    }

    /**
     * Returns true if there is column projection.
     *
     * @return true if there is column projection, false otherwise
     */
    public boolean hasColumnProjection() {
        return numAttrsProjected > 0 && numAttrsProjected < tupleDescription.size();
    }

    /**
     * Returns the number of columns in tuple description.
     *
     * @return number of columns
     */
    public int getColumns() {
        return tupleDescription.size();
    }

    /**
     * Returns column index from tuple description.
     *
     * @param index index of column
     * @return column by index
     */
    public ColumnDescriptor getColumn(int index) {
        return tupleDescription.get(index);
    }

    public Optional<ColumnDescriptor> getColumn(String name) {
        for (ColumnDescriptor column : tupleDescription) {
            if (column.columnName().equals(name)) {
                return Optional.of(column);
            }
        }

        return Optional.empty();
    }

    /**
     * Sets the name of the server in a multi-server setup.
     * If the name is blank, it is defaulted to "default"
     *
     * @param serverName the name of the server
     */
    public void setServerName(String serverName) {
        if (StringUtils.isNotBlank(serverName)) {

            if (!Utilities.isValidRestrictedDirectoryName(serverName)) {
                throw new IllegalArgumentException(String.format("Invalid server name '%s'", serverName));
            }

            this.serverName = serverName.toLowerCase();
        }
    }

    public void validate() {
        // accessor and resolver are user properties, might be missing if profile is not set
        ensureNotNull("METADATA", metadataFetcher);
    }

    public long getSplitSize() {
        if (splitSize <= 128) {
            return 128 * 1024 * 1024;
        }

        if (splitSize > 1024) {
            return 1024 * 1024 * 1024;
        }

        return splitSize * 1024 * 1204;
    }

    public void setSplitSize(String splitSize) {
        if (splitSize == null) {
            return;
        }

        this.splitSize = Integer.parseInt(splitSize);
    }

    public String getDataSource() {
        return dataSource == null ? path : dataSource;
    }

    public String getQueryType() {
        return queryType == null ? "snapshot" : queryType.toLowerCase();
    }

    public boolean isMetadataTableEnabled() {
        if (metadataTableEnabled == null) {
            return true;
        }

        if (metadataTableEnabled.toLowerCase().equals("true")) {
            return true;
        }

        return false;
    }

    private void ensureNotNull(String property, Object value) {
        if (value == null) {
            fail("Property %s has no value in the current request", property);
        }
    }

    private void fail(String message, Object... args) {
        String errorMessage = String.format(message, args);
        throw new IllegalArgumentException(errorMessage);
    }
}
