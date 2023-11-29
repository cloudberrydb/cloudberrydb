package cn.hashdata.dlagent.service;

import cn.hashdata.dlagent.api.utilities.ColumnDescriptor;
import org.apache.commons.lang.StringUtils;
import cn.hashdata.dlagent.api.error.DlRuntimeException;
import cn.hashdata.dlagent.api.model.PluginConf;
import cn.hashdata.dlagent.api.model.RequestContext;
import cn.hashdata.dlagent.api.utilities.CharsetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.info.BuildProperties;
import org.springframework.stereotype.Component;
import org.springframework.util.MultiValueMap;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Parser for HTTP requests that contain data in HTTP headers.
 */
@Component
public class HttpRequestParser implements RequestParser<MultiValueMap<String, String>> {
    private static final Logger LOG = LoggerFactory.getLogger(HttpRequestParser.class);

    private static final String TRUE_LCASE = "true";

    private final CharsetUtils charsetUtils;
    private final PluginConf pluginConf;
    private final HttpHeaderDecoder headerDecoder;
    private final BuildProperties buildProperties;

    /**
     * Create a new instance of the HttpRequestParser with the given PluginConf
     *
     * @param pluginConf    the plugin conf
     * @param charsetUtils  utilities for Charset
     * @param headerDecoder decoder of http headers
     * @param buildProperties   Spring Boot build info
     */
    public HttpRequestParser(PluginConf pluginConf, CharsetUtils charsetUtils, HttpHeaderDecoder headerDecoder, BuildProperties buildProperties) {
        this.pluginConf = pluginConf;
        this.charsetUtils = charsetUtils;
        this.headerDecoder = headerDecoder;
        this.buildProperties = buildProperties;
    }

    /**
     * Throws an exception when the given property value is missing in request.
     *
     * @param property missing property name
     * @throws IllegalArgumentException throws an exception with the property
     *                                  name in the error message
     */
    private static void protocolViolation(String property) {
        String error = String.format("Property %s has no value in the current request", property);
        throw new IllegalArgumentException(error);
    }

    @Override
    public RequestContext parseRequest(MultiValueMap<String, String> requestHeaders) {
        RequestMap params = new RequestMap(requestHeaders, headerDecoder);
        if (LOG.isDebugEnabled()) {
            // Logging only keys to prevent sensitive data to be logged
            LOG.debug("Parsing request parameters: " + params.keySet());
        }

        RequestContext context = new RequestContext();

        // first of all, set profile and enrich parameters with information from specified profile
        String profile = params.removeUserProperty("PROFILE").toLowerCase();
        context.setProfile(profile);
        addProfilePlugins(profile, params);

        context.setMethod(params.removeUserProperty("METHOD"));
        context.setConfig(params.removeUserProperty("CONFIG"));
        context.setServerName(params.removeUserProperty("SERVER"));
        context.setPath(params.removeProperty("DATA-DIR"));
        context.setDataSource(params.removeUserProperty("DATA-SOURCE"));

        context.setCatalogType(params.removeUserProperty("CATALOG-TYPE"));
        context.setSplitSize(params.removeUserProperty("SPLIT-SIZE"));
        context.setQueryType(params.removeUserProperty("QUERY-TYPE"));
        context.setMetadataTableEnabled(params.removeUserProperty("METADATA-TABLE-ENABLE"));

        String filterString = params.removeOptionalProperty("FILTER");
        String hasFilter = params.removeProperty("HAS-FILTER");
        if (filterString != null) {
            context.setFilterString(filterString);
        } else if ("1".equals(hasFilter)) {
            LOG.info("Original query has filter, but it was not propagated to datalake-proxy");
        }

        context.setMetadataFetcher(params.removeUserProperty("METADATA"));

        context.setSchemaName(params.removeProperty("SCHEMA-NAME"));
        context.setTableName(params.removeProperty("TABLE-NAME"));
        context.setTotalSegments(params.removeIntProperty("SEGMENT-COUNT"));
        context.setTransactionId(params.removeProperty("XID"));

        // parse tuple description
        parseTupleDescription(params, context);

        context.setGpSessionId(params.removeIntProperty("SESSION-ID"));
        context.setGpCommandCount(params.removeIntProperty("COMMAND-COUNT"));
        context.setUser(params.removeProperty("USER"));
        context.setPluginConf(pluginConf);

        // validate that the result has all required fields, and values are in valid ranges
        context.validate();

        return context;
    }

    /*
     * Sets the tuple description for the record
     * Attribute Projection information is optional
     */
    private void parseTupleDescription(RequestMap params, RequestContext context) {
        int columns = params.removeOptionalIntProperty("ATTRS");
        BitSet attrsProjected = new BitSet(columns + 1);

        /* Process column projection info */
        String columnProjStr = params.removeOptionalProperty("ATTRS-PROJ");
        if (columnProjStr != null) {
            int numberOfProjectedColumns = Integer.parseInt(columnProjStr);
            context.setNumAttrsProjected(numberOfProjectedColumns);
            if (numberOfProjectedColumns > 0) {
                String[] projectionIndices = params.removeProperty("ATTRS-PROJ-IDX").split(",");
                for (String s : projectionIndices) {
                    attrsProjected.set(Integer.parseInt(s));
                }
            } else {
                /* This is a special case to handle aggregate queries not related to any specific column
                 * eg: count(*) queries. */
                attrsProjected.set(0);
            }
        }

        for (int attrNumber = 0; attrNumber < columns; attrNumber++) {
            String columnName = params.removeProperty("ATTR-NAME" + attrNumber);
            int columnOID = params.removeIntProperty("ATTR-TYPECODE" + attrNumber);
            String columnTypeName = params.removeProperty("ATTR-TYPENAME" + attrNumber);
            Integer[] columnTypeMods = parseTypeMods(params, attrNumber);
            // Project the column if columnProjStr is null
            boolean isProjected = columnProjStr == null || attrsProjected.get(attrNumber);
            ColumnDescriptor column = new ColumnDescriptor(
                    columnName,
                    columnOID,
                    attrNumber,
                    columnTypeName,
                    columnTypeMods,
                    isProjected);
            context.getTupleDescription().add(column);
        }
    }

    private Integer[] parseTypeMods(RequestMap params, int columnIndex) {
        String typeModCountPropName = "ATTR-TYPEMOD" + columnIndex + "-COUNT";
        String typeModeCountStr = params.removeOptionalProperty(typeModCountPropName);
        if (typeModeCountStr == null)
            return null;

        int typeModeCount = parsePositiveIntOrError(typeModeCountStr, typeModCountPropName);

        Integer[] result = new Integer[typeModeCount];
        for (int i = 0; i < typeModeCount; i++) {
            String typeModItemPropName = "ATTR-TYPEMOD" + columnIndex + "-" + i;
            result[i] = parsePositiveIntOrError(params.removeProperty(typeModItemPropName), typeModItemPropName);
        }
        return result;
    }

    private int parsePositiveIntOrError(String s, String propName) {
        int n;
        try {
            n = Integer.parseInt(s);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(String.format("%s must be an integer", propName), e);
        }
        if (n < 0) {
            throw new IllegalArgumentException(String.format("%s must be a positive integer", propName));
        }
        return n;
    }

    /**
     * Sets the requested profile plugins from the profile file into the request map.
     *
     * @param profile profile specified in the user request
     * @param params  parameters provided in the user request
     */
    private void addProfilePlugins(String profile, RequestMap params) {
        // if profile was not specified in the request, do nothing
        if (StringUtils.isBlank(profile)) {
            LOG.debug("Profile was not specified in the request");
            return;
        }

        LOG.debug("Adding plugins for profile {}", profile);

        // get Profile's plugins from the configuration file
        Map<String, String> pluginsMap = pluginConf.getPlugins(profile);

        // create sets of keys to find out duplicates between what user has specified in the request
        // and what is configured in the configuration file -- DO NOT ALLOW DUPLICATES
        List<String> duplicates = pluginsMap.keySet().stream()
                .filter(n -> params.containsKey(RequestMap.USER_PROP_PREFIX + n))
                .collect(Collectors.toList());

        if (!duplicates.isEmpty()) {
            throw new IllegalArgumentException(String.format("Profile '%s' already defines: %s",
                    profile, duplicates));
        }

        // since there are guaranteed to be no duplications at this point,
        // add properties defined by profiles to the request map as if they were specified by the user
        pluginsMap.forEach((k, v) -> params.put(RequestMap.USER_PROP_PREFIX + k, v));
    }

    /**
     * Converts the request headers multivalued map to a case-insensitive
     * regular map by taking only first values and storing them in a
     * CASE_INSENSITIVE_ORDER TreeMap. All values are converted from ISO_8859_1
     * (ISO-LATIN-1) to UTF_8.
     */
    static class RequestMap extends TreeMap<String, String> {
        private static final long serialVersionUID = 4745394510220213936L;
        private static final String PROP_PREFIX = "X-GP-";
        private static final String USER_PROP_PREFIX = "X-GP-OPTIONS-";
        private static final String USER_PROP_PREFIX_LOWERCASE = "x-gp-options-";


        RequestMap(MultiValueMap<String, String> requestHeaders, HttpHeaderDecoder headerDecoder) {
            super(String.CASE_INSENSITIVE_ORDER);

            boolean headersEncoded = headerDecoder.areHeadersEncoded(requestHeaders);
            for (Map.Entry<String, List<String>> entry : requestHeaders.entrySet()) {
                String key = entry.getKey();
                String value = headerDecoder.getHeaderValue(key, entry.getValue(), headersEncoded);
                if (value != null) {
                    LOG.trace("Key: {} Value: {}", key, value);
                    put(key, value);
                }
            }
        }

        /**
         * Returns a user defined property.
         *
         * @param userProp the lookup user property
         * @return property value as a String
         */
        private String removeUserProperty(String userProp) {
            return remove(USER_PROP_PREFIX + userProp);
        }

        /**
         * Returns the optional property value. Unlike {@link #removeProperty}, it will
         * not fail if the property is not found. It will just return null instead.
         *
         * @param property the lookup optional property
         * @return property value as a String
         */
        private String removeOptionalProperty(String property) {
            return remove(PROP_PREFIX + property);
        }

        /**
         * Returns a property value as an int type and removes it from the map
         *
         * @param property the lookup property
         * @return property value as an int type
         * @throws NumberFormatException if the value is missing or can't be
         *                               represented by an Integer
         */
        private int removeIntProperty(String property) {
            return Integer.parseInt(removeProperty(property));
        }

        private int removeOptionalIntProperty(String property) {
            String result = removeOptionalProperty(property);
            if (result != null) {
                return Integer.parseInt(result);
            }

            return 0;
        }

        /**
         * Returns the value to which the specified property is mapped and
         * removes it from the map
         *
         * @param property the lookup property key
         * @throws IllegalArgumentException if property key is missing
         */
        private String removeProperty(String property) {
            String result = remove(PROP_PREFIX + property);

            if (result == null) {
                protocolViolation(property);
            }

            return result;
        }
    }
}
