import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.Socket;
import java.net.URI;
import java.util.*;

public final class HMSClient {
    private static Logger LOG = LoggerFactory.getLogger(HMSClient.class);
    private static final short ALL_PARTS = -1;
    private HiveMetaStoreClient client;
    private String errorMessage;

    public int openConnection(String uri, MessageBuffer errMessage, int debug) {
        try {
            HiveConf hiveConf = new HiveConf();
            hiveConf.set("hive.metastore.uris", uri);

            client = new HiveMetaStoreClient(hiveConf);

            LOG.info("connect successfully");
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

            String servPrinc = selectServPrincipal(servicePrincipal, uri);

            HiveConf hiveConf = new HiveConf();
            hiveConf.set("hive.metastore.uris", uri);
            hiveConf.set("hive.metastore.kerberos.principal", servPrinc);
            hiveConf.set("hive.metastore.sasl.enabled", "true");
            hiveConf.set("hadoop.security.authentication", "kerberos");

            UserGroupInformation.setConfiguration(hiveConf);
            UserGroupInformation.loginUserFromKeytab(clientPrincipal, clientKeytab);

            client = new HiveMetaStoreClient(hiveConf);
        } catch (Exception e) {
            LOG.error("failed to connect metastore service: {}", e.toString());
            errMessage.catVal(exceptionStacktraceToString(e));
            return -1;
        }

        return 0;
    }

    public int tableExists(String dbName, String tableName, MessageBuffer result, MessageBuffer errMessage) {
        try {
            boolean found = client.tableExists(dbName, tableName);
            if (found) {
                result.catVal(1);
            } else {
                result.catVal(0);
            }
        } catch (Exception e) {
            LOG.error("failed to execute tableExists: {}", e.toString());
            errMessage.catVal(e.toString());
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

    private boolean isNumericType(String type) {
        switch (type) {
            case "smallint":
            case "integer":
            case "bigint":
            case "float":
            case "double precision":
                return true;
            default:
                return false;
        }
    }

    private String formFields(Table table) throws Exception {
        MessageBuffer columnBuf = new MessageBuffer("");
        List<FieldSchema> schemas = table.getSd().getCols();

        for (int i = 0; i < schemas.size(); i++) {
            String name = schemas.get(i).getName();
            String type = typeConversion(schemas.get(i).getType());

            if (i == schemas.size() - 1) {
                columnBuf.catVal(name + " " + type);
            } else {
                columnBuf.catVal(name + " " + type + ", ");
            }
        }

        return columnBuf.getVal();
    }

    private void formKeyField(Partition partition,
                                  String name,
                                  String type,
                                  int index,
                                  boolean isLastKey,
                                  MessageBuffer columnBuf) {
        if (partition == null) {
            columnBuf.catVal(name + " " + type + (isLastKey ? "" : ", "));
            return;
        }

        if (isNumericType(type)) {
            columnBuf.catVal(name + " " + type + " default " + partition.getValues().get(index));
        } else {
            columnBuf.catVal(name + " " + type + " default '" + partition.getValues().get(index) +"'");
        }

        columnBuf.catVal( (isLastKey ? "" : ", "));
    }

    private String formFieldWithKeys(Table table, Partition partition) throws Exception {
        MessageBuffer columnBuf = new MessageBuffer("");

        List<FieldSchema> schemas = table.getSd().getCols();
        List<FieldSchema> keys = table.getPartitionKeys();

        for (int i = 0; i < schemas.size(); i++) {
            columnBuf.catVal(schemas.get(i).getName() + " "
                    + typeConversion(schemas.get(i).getType()) + ", ");
        }

        for (int i = 0; i < keys.size(); i++) {
            String name = keys.get(i).getName();
            String type = partKeyTypeConversion(keys.get(i).getType());

            if (i == keys.size() - 1) {
                formKeyField(partition, name, type, i, true, columnBuf);
            } else {
                formKeyField(partition, name, type, i, false, columnBuf);
            }
        }

        return columnBuf.getVal();
    }

    private String formTableFields(Table table) throws Exception {
        return formFieldWithKeys(table, null);
    }

    private List<String> formPartitionFields(Table table, List<Partition> partitions) throws Exception {
        List<String> results = new ArrayList<>();

        for(Partition partition : partitions) {
            String fields = this.formFieldWithKeys(table, partition);
            results.add(fields);
        }

        return results;
    }

    private void getPartTableMetaData(Table table,
                                      String dbName,
                                      String tableName,
                                      TableMetaData metaData) throws Exception {
        List<String> columns = new ArrayList<>();
        List<String> types = new ArrayList<>();
        List<FieldSchema> keys = table.getPartitionKeys();

        for (int i = 0; i < keys.size(); i++) {
            columns.add(keys.get(i).getName());
            types.add(partKeyTypeConversion(keys.get(i).getType()));
        }

        List<Partition> partitions = client.listPartitions(dbName, tableName, ALL_PARTS);

        metaData.setPartitions(partitions);
        metaData.setPartKeys(columns);
        metaData.setPartKeyTypes(types);
        metaData.setPartFields(formPartitionFields(table, partitions));
        metaData.setField(formTableFields(table));
        metaData.setPartitionTable(true);

        setTableMetaData(table, metaData);
    }

    private void getNormalTableMetaData(Table table, TableMetaData metaData) throws Exception {
        metaData.setField(formFields(table));
        metaData.setPartitionTable(false);

        setTableMetaData(table, metaData);
    }

    private String formTextParameters(Map<String, String> parameters) {
        StringBuffer message = new StringBuffer();

        message.append("(DELIMITER '");
        if (parameters.containsKey("field.delim")) {
			String s = parameters.get("field.delim");
            message.append(s);
			if (s.equals("'")) {
				message.append(s);
			}
        } else {
            message.append("\001");
        }
        message.append("' ");

        message.append("NULL '");
        if (parameters.containsKey("serialization.null.format")) {
            String s = parameters.get("serialization.null.format");
            message.append(s);
			if (s.equals("'")) {
				message.append(s);
			}
        } else {
            message.append("\\N");
        }
        message.append("' ");

        message.append("NEWLINE '");
        if (parameters.containsKey("line.delim")) {
            String lineDelim = parameters.get("line.delim");
            if (lineDelim.equals("\r")) {
                message.append("CR");
            } else if (lineDelim.equals("\n")) {
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
            String s = parameters.get("escape.delim");
            message.append(s);
			if (s.equals("'")) {
				message.append(s);
			}
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
			String s = parameters.get("quoteChar");
            message.append(s);
			if (s.equals("'")) {
				message.append(s);
			}
        } else {
            message.append("\"");
        }
        message.append("' ");

        message.append("DELIMITER '");
        if (parameters.containsKey("separatorChar")) {
            String s = parameters.get("separatorChar");
            message.append(s);
			if (s.equals("'")) {
				message.append(s);
			}
        } else {
            message.append(",");
        }
        message.append("' ");

        message.append("ESCAPE '");
        if (parameters.containsKey("escapeChar")) {
			String s = parameters.get("escapeChar");
            message.append(s);
			if (s.equals("'")) {
				message.append(s);
			}
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
            Table table = client.getTable(dbName, tableName);

            int nKeys = table.getPartitionKeysSize();

            if (nKeys > 0) {
                getPartTableMetaData(table, dbName, tableName, metaData);
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
            List<String> results = client.getTables(dbName, null);
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

    private String selectServPrincipal(String principal, String uri) throws Exception {
        URI tmpUri = new URI(uri);
        return SecurityUtil.getServerPrincipal(principal, tmpUri.getHost());
    }
}
