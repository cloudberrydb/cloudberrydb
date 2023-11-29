package cn.hashdata.dlagent.api.utilities;

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

import cn.hashdata.dlagent.api.error.UnsupportedTypeException;
import cn.hashdata.dlagent.api.io.DataType;
import cn.hashdata.dlagent.api.model.Metadata;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import cn.hashdata.dlagent.api.error.DlRuntimeException;
import cn.hashdata.dlagent.api.model.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utilities class exposes helper method for dlagent classes
 */
public class Utilities {

    private static final Logger LOG = LoggerFactory.getLogger(Utilities.class);
    private static final String PROPERTY_KEY_FRAGMENTER_CACHE = "dlagent.service.fragmenter.cache.enabled";
    private static final char[] PROHIBITED_CHARS = new char[]{'/', '\\', '.', ' ', ',', ';'};
    private static final String[] HOSTS = new String[]{"localhost"};

    /**
     * matches the scheme:// portion of a URI where the "scheme:" is optional,
     * and this entire pattern is optional
     */
    public static final Pattern SCHEME_PATTERN = Pattern.compile("^((([^:/?#]+):)?//)?");

    /**
     * matches a :, /, ?, or #
     */
    public static final Pattern NON_HOSTNAME_CHARACTERS = Pattern.compile("[:/?#]");

    /**
     * Validation for directory names that can be created
     * for the server directory.
     *
     * @param name the directory name
     * @return true if valid, false otherwise
     */
    public static boolean isValidDirectoryName(String name) {
        return isValidRestrictedDirectoryName(name, false);
    }

    /**
     * Validation for directory names that can be created
     * for the server configuration directory. Perform validation
     * for prohibited characters.
     *
     * @param name the directory name
     * @return true if valid, false otherwise
     */
    public static boolean isValidRestrictedDirectoryName(String name) {
        return isValidRestrictedDirectoryName(name, true);
    }

    /**
     * Validation for directory names that can be created
     * for the server configuration directory. Optionally checking for
     * prohibited characters in the directory name.
     *
     * @param name                    the directory name
     * @param checkForProhibitedChars true to check for prohibited chars, false otherwise
     * @return true if value, false otherwise
     */
    private static boolean isValidRestrictedDirectoryName(String name, boolean checkForProhibitedChars) {
        if (StringUtils.isBlank(name) ||
                (checkForProhibitedChars && StringUtils.containsAny(name, PROHIBITED_CHARS))) {
            return false;
        }
        File file = new File(name);
        try {
            file.getCanonicalPath();
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * Creates an object using the class name. The class name has to be a class
     * located in the webapp's CLASSPATH.
     *
     * @param confClass the class of the metaData used to initialize the
     *                  instance
     * @param className a class name to be initialized.
     * @param metaData  input data used to initialize the class
     * @return Initialized instance of given className
     * @throws Exception throws exception if classname was not found in
     *                   classpath, didn't have expected constructor or failed to be
     *                   instantiated
     */
    public static Object createAnyInstance(Class<?> confClass,
                                           String className, RequestContext metaData)
            throws Exception {

        Class<?> cls;
        try {
            cls = Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw e;
        }

        Constructor<?> con = cls.getConstructor(confClass);

        return instantiate(con, metaData);
    }

    /**
     * Creates an object using the class name with its default constructor. The
     * class name has to be a class located in the webapp's CLASSPATH.
     *
     * @param className a class name to be initialized
     * @return initialized instance of given className
     * @throws Exception throws exception if classname was not found in
     *                   classpath, didn't have expected constructor or failed to be
     *                   instantiated
     */
    public static Object createAnyInstance(String className) throws Exception {
        Class<?> cls = Class.forName(className);
        Constructor<?> con = cls.getConstructor();
        return instantiate(con);
    }

    private static Object instantiate(Constructor<?> con, Object... args)
            throws Exception {
        try {
            return con.newInstance(args);
        } catch (InvocationTargetException e) {
            /*
             * We are creating resolvers, accessors, fragmenters, etc. using the
             * reflection framework. If for example, a resolver, during its
             * instantiation - in the c'tor, will throw an exception, the
             * Resolver's exception will reach the Reflection layer and there it
             * will be wrapped inside an InvocationTargetException. Here we are
             * above the Reflection layer and we need to unwrap the Resolver's
             * initial exception and throw it instead of the wrapper
             * InvocationTargetException so that our initial Exception text will
             * be displayed in psql instead of the message:
             * "Internal Server Error"
             */
            throw (e.getCause() != null) ? new Exception(e.getCause()) : e;
        }
    }

    /**
     * Transforms a byte array into a string of octal codes in the form
     * \\xyz\\xyz
     * <p>
     * We double escape each char because it is required in postgres bytea for
     * some bytes. In the minimum all non-printables, backslash, null and single
     * quote. Easier to just escape everything see
     * http://www.postgresql.org/docs/9.0/static/datatype-binary.html
     * <p>
     * Octal codes must be padded to 3 characters (001, 012)
     *
     * @param bytes bytes to escape
     * @param sb    octal codes of given bytes
     */
    public static void byteArrayToOctalString(byte[] bytes, StringBuilder sb) {
        if ((bytes == null) || (sb == null)) {
            return;
        }

        sb.ensureCapacity(sb.length()
                + (bytes.length * 5 /* characters per byte */));
        for (int b : bytes) {
            sb.append(String.format("\\\\%03o", b & 0xff));
        }
    }

    /**
     * Replaces any non-alpha-numeric character with a '.'. This measure is used
     * to prevent cross-site scripting (XSS) when an input string might include
     * code or script. By removing all special characters and returning a
     * censured string to the user this threat is avoided.
     *
     * @param input string to be masked
     * @return masked string
     */
    public static String maskNonPrintables(String input) {
        if (StringUtils.isEmpty(input)) {
            return input;
        }
        return input.replaceAll("[^a-zA-Z0-9_:/-]", ".");
    }

    /**
     * Determines whether a class with a given name implements a specific interface.
     *
     * @param className name of the class
     * @param iface     class of the interface
     * @return true if the class implements the interface, false otherwise
     */
    public static boolean implementsInterface(String className, Class<?> iface) {
        boolean result = false;
        try {
            result = iface.isAssignableFrom(Class.forName(className));
        } catch (ClassNotFoundException e) {
            LOG.error("Unable to load class: {}", e.getMessage());
        }
        return result;
    }

    /**
     * Data sources are absolute data paths. Method ensures that dataSource
     * begins with '/' unless the path includes the protocol as a prefix
     *
     * @param dataSource The path to a file or directory of interest.
     *                   Retrieved from the client request.
     * @return an absolute data path
     */
    public static String absoluteDataPath(String dataSource) {
        return (dataSource.charAt(0) == '/') ? dataSource : "/" + dataSource;
    }

    /**
     * Determine whether the configuration is using Kerberos to
     * establish user identities or is relying on simple authentication
     *
     * @param configuration the configuration for a given server
     * @return true if the given configuration is for a secure environment
     */
    public static boolean isSecurityEnabled(Configuration configuration) {
        return SecurityUtil.getAuthenticationMethod(configuration) !=
                UserGroupInformation.AuthenticationMethod.SIMPLE;
    }

    /**
     * Right trim whitespace on a string (it does not trim tabs)
     * <p>
     *     <ul>
     *         <li>null returns null</li>
     *         <li>"abc" returns "abc"</li>
     *         <li>" abc" returns " abc"</li>
     *         <li>"abc " returns "abc"</li>
     *         <li>"    " returns ""</li>
     *         <li>"abc \t " returns "abc \t"</li>
     *         <li>"abc \t\t" returns "abc \t\t"</li>
     *     </ul>
     * </p>
     *
     * @param s the string
     * @return the right trimmed string
     */
    public static String rightTrimWhiteSpace(String s) {
        if (s == null) return null;
        int length = s.length();
        while (length > 0 && s.charAt(length - 1) == ' ') length--;

        if (length == s.length()) return s;
        if (length == 0) return "";
        return s.substring(0, length);
    }

    /**
     * Returns the hostname from a given URI string
     *
     * @param uri the URI string
     * @return the hostname from a given URI string
     */
    public static String getHost(String uri) {
        if (StringUtils.isBlank(uri))
            return null;

        int start = 0, end = uri.length();
        Matcher matcher = SCHEME_PATTERN.matcher(uri);

        if (matcher.find()) {
            // Get the start of the hostname
            start = matcher.end();
        }

        matcher = NON_HOSTNAME_CHARACTERS.matcher(uri).region(start, uri.length());

        if (matcher.find()) {
            // Get the end of the hostname
            end = matcher.start();
        }

        return (end > start) ? uri.substring(start, end) : null;
    }

    /**
     * Parses a boolean property such that if the property has a value other than true or false, an exception is thrown
     * @param configuration the configuration to search for the given property name
     * @param propertyName the name of the property to parse
     * @param defaultValue the default value if the property is not defined
     * @return the parsed property value if the property is defined, else the default
     */
    public static boolean parseBooleanProperty(Configuration configuration, String propertyName, boolean defaultValue) {
        // do not use getBoolean as it would return default value for an invalid property value
        String propertyValue = configuration.get(propertyName);
        if (propertyValue == null) {
            return defaultValue;
        }

        propertyValue = propertyValue.trim();
        if (propertyValue.equalsIgnoreCase("true")) {
            return true;
        } else if (propertyValue.equalsIgnoreCase("false")) {
            return  false;
        } else {
            throw new DlRuntimeException(String.format(
                    "Property %s has invalid value '%s'; value should be either 'true' or 'false'", propertyName, propertyValue
            ));
        }
    }

    /**
     * Verifies modifiers are null or integers.
     * Modifier is a value assigned to a type,
     * e.g. size of a varchar - varchar(size).
     *
     * @param modifiers type modifiers to be verified
     * @return whether modifiers are null or integers
     */
    public static boolean verifyIntegerModifiers(String[] modifiers) {
        if (modifiers == null) {
            return true;
        }
        for (String modifier : modifiers) {
            if (StringUtils.isBlank(modifier) || !StringUtils.isNumeric(modifier)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Extracts the db_name and table_name from the qualifiedName.
     * qualifiedName is the table name that the user enters in the CREATE EXTERNAL TABLE statement
     * or when querying table.
     * It can be either <code>table_name</code> or <code>db_name.table_name</code>.
     *
     * @param qualifiedName table name
     * @return {@link Metadata.Item} object holding the full table name
     */
    public static Metadata.Item extractTableFromName(String qualifiedName) {
        String dbPattern, tablePattern;
        String errorMsg = " is not a valid table name. "
                + "Should be either <table_name> or <db_name.table_name>";

        if (StringUtils.isBlank(qualifiedName)) {
            throw new IllegalArgumentException("empty string" + errorMsg);
        }

        String[] rawTokens = qualifiedName.split("[.]");
        ArrayList<String> tokens = new ArrayList<>();
        for (String tok : rawTokens) {
            if (StringUtils.isBlank(tok)) {
                continue;
            }
            tokens.add(tok.trim());
        }

        if (tokens.size() == 1) {
            dbPattern = "default";
            tablePattern = tokens.get(0);
        } else if (tokens.size() == 2) {
            dbPattern = tokens.get(0);
            tablePattern = tokens.get(1);
        } else {
            throw new IllegalArgumentException("\"" + qualifiedName + "\"" + errorMsg);
        }

        return new Metadata.Item(dbPattern, tablePattern);
    }

    public static Object boxLiteral(Object literal) {
        if (literal instanceof String ||
                literal instanceof Long ||
                literal instanceof Double ||
                literal instanceof Date ||
                literal instanceof Timestamp ||
                literal instanceof BigDecimal ||
                literal instanceof Boolean) {
            return literal;
        } else if (literal instanceof Byte ||
                literal instanceof Short ||
                literal instanceof Integer) {
            return ((Number) literal).longValue();
        } else if (literal instanceof Float) {
            // to avoid change in precision when upcasting float to double
            // we convert the literal to string and parse it as double.
            return Double.parseDouble(literal.toString());
        } else {
            throw new IllegalArgumentException("Unknown type for literal " +
                    literal);
        }
    }

    /**
     * Converts the string value to the given type
     *
     * @param dataType the data type
     * @param value    the value
     * @return the string value to the given type
     */
    public static Object convertDataValue(DataType dataType, String value) {
        try {
            switch (dataType) {
                case BIGINT:
                    return Long.parseLong(value);
                case INTEGER:
                case SMALLINT:
                    return Integer.parseInt(value);
                case REAL:
                    return Float.parseFloat(value);
                case NUMERIC:
                    return BigDecimal.valueOf(Double.parseDouble(value));
                case FLOAT8:
                    return Double.parseDouble(value);
                case TEXT:
                case VARCHAR:
                case BPCHAR:
                    return value;
                case BOOLEAN:
                    return Boolean.parseBoolean(value);
                case DATE:
                    return Date.valueOf(value);
                case TIMESTAMP_WITH_TIME_ZONE:
                case TIMESTAMP:
                    return Timestamp.valueOf(value);
                case TIME:
                    return Time.valueOf(value);
                case BYTEA:
                    return value.getBytes();
                default:
                    throw new UnsupportedTypeException(String.format("DataType %s unsupported", dataType));
            }
        } catch (NumberFormatException nfe) {
            throw new IllegalStateException(String.format("failed to parse number data %s for type %s", value, dataType));
        }
    }
}
