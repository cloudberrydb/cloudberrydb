import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClientCompatibility2xx;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.HCUserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.slf4j.Logger;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.Socket;
import java.net.InetAddress;
import java.net.URI;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;

public final class HMSClient {
    private static Logger LOG = LoggerFactory.getLogger(HMSClient.class);
    private HiveMetaStoreClient client;
    private String errorMessage;

    public int openConnection(String uri, MessageBuffer errMessage, int debug) {
        try {
            HiveConf hiveConf = new HiveConf();
            hiveConf.set("hive.metastore.uris", uri);

            HCUserGroupInformation.reset();
            client = new HiveMetaStoreClientCompatibility2xx(hiveConf);

        } catch (Exception e) {
            LOG.error("failed to connect metastore service: {}", e.toString());
            errMessage.catVal(exceptionStacktraceToString(e));
            return -1;
        }

        return 0;
    }

    public int openConnection(String uris,
                              String servicePrincipal,
                              String clientPrincipal,
                              String clientKeytab,
                              String rpcProtection,
                              MessageBuffer errMessage,
                              int debug) {
        try {
            if (debug == 1) {
                System.setProperty("sun.security.krb5.debug", "true");
            }
            System.setProperty("java.security.krb5.conf", "/etc/krb5.conf");

            String uri = selectUri(uris);
            if (uri == null) {
                throw new Exception("failed to connect to meta store using any of the URIs provided. " +
                "Most recent failure: " + this.errorMessage);
            }

            String servPrinc = selectServPrincipal(servicePrincipal);

            HiveConf hiveConf = new HiveConf();
            hiveConf.set("hive.metastore.uris", uri);
            hiveConf.set("hive.metastore.kerberos.principal", servPrinc);
            hiveConf.set("hive.metastore.sasl.enabled", "true");
            hiveConf.set("hadoop.security.authentication", "kerberos");
            hiveConf.set("hadoop.rpc.protection", rpcProtection);
            hiveConf.set("hadoop.security.auth_to_local", "RULE:[1:$1] RULE:[2:$1] DEFAULT");

            HCUserGroupInformation.reset();
            UserGroupInformation.setConfiguration(hiveConf);
            UserGroupInformation.loginUserFromKeytab(clientPrincipal, clientKeytab);

            client = new HiveMetaStoreClientCompatibility2xx(hiveConf);
        } catch (Exception e) {
            LOG.error("failed to connect metastore service: {}", e.toString());
            errMessage.catVal(exceptionStacktraceToString(e));
            return -1;
        }

        return 0;
    }

    private String typeConversion(String sourceType) throws Exception {
        switch (sourceType) {
            case "binary":
                return "bytea";
            case "tinyint":
                return "smallint";
            case "smallint":
                return "smallint";
            case "int":
                return "integer";
            case "bigint":
                return "bigint";
            case "float":
                return "float4";
            case "double":
                return "double precision";
            case "string":
                return "text";
            case "timestamp":
                return "timestamp";
            case "date":
                return "date";
            default:
                if (sourceType.startsWith("char")
                        ||sourceType.startsWith("varchar")
                        ||sourceType.startsWith("decimal")) {
                    return sourceType;
                } else if (sourceType.startsWith("array<")
                        ||sourceType.startsWith("struct<")
                        ||sourceType.startsWith("map<")) {
                    return "text";
                } else {
                    throw new Exception("data type \""+ sourceType + "\" is not supported");
                }
        }
    }

    private String partKeyTypeConversion(String sourceType) throws Exception {
        switch (sourceType) {
            case "tinyint":
                return "smallint";
            case "smallint":
                return "smallint";
            case "int":
                return "integer";
            case "bigint":
                return "bigint";
            case "float":
                return "float4";
            case "double":
                return "double precision";
            case "string":
                return "text";
            case "date":
                return "text";
            default:
                // Do not support char and char(n) due to problem of HMS
				if (sourceType.startsWith("varchar")) {
					return "text";
				} else if (sourceType.startsWith("decimal")) {
                    return sourceType;
                } else {
                    throw new Exception("does not support \"" + sourceType + " type\" as a partition key type");
                }
        }
    }

    private String formColumnComment(String columnName, String columnComment) {
        return "comment on column %s." + columnName + " is '" + columnComment + "'";
    }

    private String formFields(Table table, List<String> comments) throws Exception {
        MessageBuffer columnBuf = new MessageBuffer("");
        List<FieldSchema> schemas = table.getSd().getCols();

        for (int i = 0; i < schemas.size(); i++) {
            String name = schemas.get(i).getName();
            String comment = schemas.get(i).getComment();
            String type = typeConversion(schemas.get(i).getType());

            if (i == schemas.size() - 1) {
                columnBuf.catVal(name + " " + type);
            } else {
                columnBuf.catVal(name + " " + type + ", ");
            }
            if (comment != null) {
                comments.add(formColumnComment(name, comment));
            }
        }

        return columnBuf.getVal();
    }

    private String formFieldWithKeys(Table table, List<String> comments) throws Exception {
        MessageBuffer columnBuf = new MessageBuffer("");

        List<FieldSchema> schemas = table.getSd().getCols();
        List<FieldSchema> keys = table.getPartitionKeys();

        for (int i = 0; i < schemas.size(); i++) {
            String name = schemas.get(i).getName();
            String comment = schemas.get(i).getComment();
            columnBuf.catVal(schemas.get(i).getName() + " "
                    + typeConversion(schemas.get(i).getType()) + ", ");
            if (comment != null) {
                comments.add(formColumnComment(name, comment));
            }
        }

        for (int i = 0; i < keys.size(); i++) {
            String name = keys.get(i).getName();
            String comment = keys.get(i).getComment();
            String type = partKeyTypeConversion(keys.get(i).getType());

            if (i == keys.size() - 1) {
                columnBuf.catVal(name + " " + type);
            } else {
                columnBuf.catVal(name + " " + type + ", ");
            }

            if (comment != null) {
                comments.add(formColumnComment(name, comment));
            }
        }

        return columnBuf.getVal();
    }

    private void getPartTableMetaData(Table table,
                                      TableMetaData metaData) throws Exception {
        List<String> columns = new ArrayList<>();
        List<String> comments = new ArrayList<>();
        List<FieldSchema> keys = table.getPartitionKeys();

        for (int i = 0; i < keys.size(); i++) {
            columns.add(keys.get(i).getName());
        }

        metaData.setPartKeys(columns);
        metaData.setField(formFieldWithKeys(table, comments));
        metaData.setColumnComments(comments);
        metaData.setPartitionTable(true);

        setTableMetaData(table, metaData);
    }

    private void getNormalTableMetaData(Table table, TableMetaData metaData) throws Exception {
        List<String> comments = new ArrayList<>();

        metaData.setField(formFields(table, comments));
        metaData.setColumnComments(comments);
        metaData.setPartitionTable(false);

        setTableMetaData(table, metaData);
    }

    private StringBuffer transformString(StringBuffer message, String value, String defaultValue) {
        if (value.isEmpty()) {
            message.append(defaultValue);
            return message;
        }

        message.append(value);
        if (value.equals("'")) {
            message.append(value);
        }

        return message;
    }

    private String formTextParameters(Map<String, String> parameters) {
        StringBuffer message = new StringBuffer();

        message.append("(DELIMITER '");
        if (parameters.containsKey("field.delim")) {
			transformString(message, parameters.get("field.delim"), "\001");
        } else {
            message.append("\001");
        }
        message.append("' ");

        message.append("NULL '");
        if (parameters.containsKey("serialization.null.format")) {
            transformString(message, parameters.get("serialization.null.format"), "\\N");
        } else {
            message.append("\\N");
        }
        message.append("' ");

        message.append("NEWLINE '");
        if (parameters.containsKey("line.delim")) {
            String lineDelim = parameters.get("line.delim");
            if (lineDelim.equals("\r")) {
                message.append("CR");
            } else if (lineDelim.equals("\n") || lineDelim.isEmpty()) {
                message.append("LF");
            } else {
                message.append("CRLF");
            }
        } else {
            message.append("LF");
        }
        message.append("' ");

        message.append("ESCAPE '");
        if (parameters.containsKey("escape.delim")) {
            transformString(message, parameters.get("escape.delim"), "OFF");
        } else {
            message.append("OFF");
        }
        message.append("')");

        return message.toString();
    }

    private String formCsvParameters(Map<String, String> parameters) {
        StringBuffer message = new StringBuffer();

        message.append("(QUOTE '");
        if (parameters.containsKey("quoteChar")) {
			transformString(message, parameters.get("quoteChar"), "\"");
        } else {
            message.append("\"");
        }
        message.append("' ");

        message.append("DELIMITER '");
        if (parameters.containsKey("separatorChar")) {
            transformString(message, parameters.get("separatorChar"), ",");
        } else {
            message.append(",");
        }
        message.append("' ");

        message.append("ESCAPE '");
        if (parameters.containsKey("escapeChar")) {
			transformString(message, parameters.get("escapeChar"), "\\");
        } else {
            message.append("\\");
        }
        message.append("')");

        return message.toString();
    }

    private void setTableMetaData(Table table, TableMetaData metaData) {
        StorageDescriptor sd = table.getSd();
        String format = sd.getInputFormat();
        String serialLib = sd.getSerdeInfo().getSerializationLib();
        Map<String, String> tabParameters = table.getParameters();

        String isTransactionalTable = tabParameters.get("transactional");
        if (isTransactionalTable != null && isTransactionalTable.toLowerCase().equals("true")) {
            metaData.setIsTransactionalTable(true);
        } else {
            metaData.setIsTransactionalTable(false);
        }

        metaData.setFormat(format);
        metaData.setTableType(table.getTableType());
        metaData.setSerialLib(serialLib);
        metaData.setLocation(sd.getLocation());

        Map<String, String> serdeParameters = sd.getSerdeInfo().getParameters();

        if (format.equals("org.apache.hadoop.mapred.TextInputFormat")) {
            if (serialLib.equals("org.apache.hadoop.hive.serde2.OpenCSVSerde")) {
                metaData.setParameters(this.formCsvParameters(serdeParameters));
            } else if (serialLib.equals("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")) {
                metaData.setParameters(this.formTextParameters(serdeParameters));
            }
        }
    }

    public int getTableMetaData(String dbName,
                                String tableName,
                                TableMetaData metaData,
                                MessageBuffer errMessage) {
        try {
            Table table;
            List<String> items = extractItemFromPattern(dbName);

            if (items.size() == 1) {
                table = client.getTable(items.get(0), tableName);
            } else {
                table = client.getTable(items.get(0), items.get(1), tableName);
            }

            int nKeys = table.getPartitionKeysSize();

            if (nKeys > 0) {
                getPartTableMetaData(table, metaData);
            } else {
                getNormalTableMetaData(table, metaData);
            }
        } catch (Exception e) {
            LOG.error("failed to execute getPartTableMetaData: {}", e.toString());
            errMessage.catVal(exceptionStacktraceToString(e));
            return -1;
        }

        return 0;
    }

    public Object[] getTables(String dbName, MessageBuffer errMessage) {
        try {
            List<String> results;
            String tablePattern = null;
            List<String> items = extractItemFromPattern(dbName);

            if (items.size() == 1) {
                results = client.getTables(items.get(0), tablePattern);
            } else {
                results = client.getTables(items.get(0), items.get(1), tablePattern);
            }

            return results.toArray();
        } catch (Exception e) {
            LOG.error("failed to execute getTables: {}", e.toString());
            errMessage.catVal(e.toString());
        }

        return null;
    }

    public void closeConnection() {
        if (client != null) {
            client.close();
        }
    }

    public static String exceptionStacktraceToString(Exception e)
    {
        ByteArrayOutputStream bs = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(bs);
        e.printStackTrace(ps);
        ps.close();
        return bs.toString();
    }

    private boolean availablePort(String host, int port) {
        boolean result = false;

        try {
            (new Socket(host, port)).close();
            result = true;
        } catch (Exception e) {
            this.errorMessage = exceptionStacktraceToString(e);
        }

        return result;
    }

    private String selectUri(String uris) throws Exception{
        String result = null;
        String metastoreUrisString[] = uris.split(",");
        for (String s : metastoreUrisString) {
            URI tmpUri = new URI(s);
            if (tmpUri.getScheme() == null) {
                    throw new IllegalArgumentException("URI: " + s + " does not have a scheme");
            }

            if (availablePort(tmpUri.getHost(), tmpUri.getPort())) {
                result = "thrift://" + tmpUri.getHost() + ":"+ tmpUri.getPort();
                return result;
            }
        }

        return result;
    }

    private String selectServPrincipal(String principal) throws Exception {
        return SecurityUtil.getServerPrincipal(principal, InetAddress.getLocalHost().getCanonicalHostName());
    }

    private List<String> extractItemFromPattern(String pattern) throws Exception {
        String errorMsg = " is not a valid Hive db name. "
                + "Should be either <db_name> or <catalog_name.db_name>";
        ArrayList<String> result = new ArrayList<>();
        String[] tokens = pattern.split("[.]");

        for (String tok : tokens) {
            if (StringUtils.isBlank(tok)) {
                continue;
            }
            result.add(tok.trim());
        }

        if (result.size() != 1 && result.size() != 2) {
            throw new IllegalArgumentException("\"" + pattern + "\"" + errorMsg);
        }

        return result;
    }
}
