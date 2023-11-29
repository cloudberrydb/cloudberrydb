package cn.hashdata.dlagent.service;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Component;
import org.springframework.util.MultiValueMap;

import javax.servlet.http.HttpServletRequest;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Service that can decode the values of http headers for dlagent client requests.
 */
@Component
public class HttpHeaderDecoder {

    private static final String PROP_PREFIX = "X-GP-";
    private static final String ENCODED_HEADER_VALUES_NAME = PROP_PREFIX + "ENCODED-HEADER-VALUES";
    private static final String UTF8_NAME = StandardCharsets.UTF_8.name();

    /**
     * Determines whether the http headers are expected to be encoded based on presence of the special dlagent http header.
     *
     * @param requestHeaders request headers
     * @return true if the header values are expected to be encoded, false otherwise
     */
    public boolean areHeadersEncoded(MultiValueMap<String, String> requestHeaders) {
        boolean headersEncoded = false;
        for (Map.Entry<String, List<String>> entry : requestHeaders.entrySet()) {
            if (StringUtils.equalsIgnoreCase(ENCODED_HEADER_VALUES_NAME, entry.getKey())) {
                headersEncoded = StringUtils.equalsIgnoreCase(flatten(entry.getValue()), "true");
                break;
            }
        }
        return headersEncoded;
    }

    /**
     * Determines whether the http headers are expected to be encoded based on presence of the special dlagent http header.
     *
     * @param request http servlet request
     * @return true if the header values are expected to be encoded, false otherwise
     */
    public boolean areHeadersEncoded(HttpServletRequest request) {
        return StringUtils.equalsIgnoreCase(flatten(Collections.list(request.getHeaders(ENCODED_HEADER_VALUES_NAME))), "true");
    }

    /**
     * Flattens the list of header values, and decodes if needed and the header name is produced by dlagent client APIs.
     *
     * @param name           header name
     * @param values         header values
     * @param headersEncoded whether the header values are expected to be encoded
     * @return single value of the header
     */
    public String getHeaderValue(String name, List<String> values, boolean headersEncoded) {
        String value = flatten(values);
        if (value != null && headersEncoded && StringUtils.startsWithIgnoreCase(name, PROP_PREFIX)) {
            try {
                return URLDecoder.decode(value, UTF8_NAME);
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(String.format("Error while URL decoding value '%s'", value), e);
            }
        }
        return value;
    }

    /**
     * Flattens the list of header values, and decodes if needed and the header name is produced by dlagent client APIs.
     *
     * @param name           header name
     * @param request        http servlet request
     * @param headersEncoded whether the header values are expected to be encoded
     * @return single value of the header
     */
    public String getHeaderValue(String name, HttpServletRequest request, boolean headersEncoded) {
        return getHeaderValue(name, Collections.list(request.getHeaders(name)), headersEncoded);
    }

    /**
     * Returns the value from the list of values. If the list has 1 element, it returns the element.
     * If the list has more than one element, it returns a flattened string joined with a comma.
     *
     * @param values the list of values
     * @return the value
     */
    private String flatten(List<String> values) {
        if (CollectionUtils.isEmpty(values)) {
            return null;
        }

        String value = values.size() > 1 ? StringUtils.join(values, ",") : values.get(0);
        if (value == null) return null;

        // Converting to value UTF-8 encoding
        return new String(value.getBytes(StandardCharsets.ISO_8859_1), StandardCharsets.UTF_8);
    }
}
