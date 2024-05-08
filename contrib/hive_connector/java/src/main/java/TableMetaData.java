import org.apache.hadoop.hive.metastore.api.Partition;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class TableMetaData {
    private List<String> partKeys;
    private String field;
    private String format;
    private boolean isPartitionTable;
    private String location;
    private String tableType;
    private String serialLib;
    private String parameters;
    private boolean isTransactionalTable;
    private List<String> columnComments;

    public Object[] getPartKeys() {
        return this.partKeys.toArray();
    }

    public Object getField() { return this.field;}

    public Object getFormat() {return this.format;}

    public int isPartitionTable() { return this.isPartitionTable ? 1 : 0; }

    public int isTransactionalTable() { return this.isTransactionalTable ? 1 : 0; }

    public Object getTableType() { return this.tableType; }

    public Object getSerialLib() { return this.serialLib; }

    public Object getParameters() { return this.parameters; }

    public Object getLocation() {return this.location; }

    public Object[] getColumnComments() { return columnComments.toArray(); }

    public void setPartKeys(List<String> partKeys) {
        this.partKeys = partKeys;
    }

    public void setField(String field) { this.field = field; }

    public void setFormat(String format) { this.format = format; }

    public void setPartitionTable(boolean isPartitionTable) { this.isPartitionTable = isPartitionTable; }

    public void setIsTransactionalTable(boolean isTransactionalTable) { this.isTransactionalTable = isTransactionalTable; }

    public void setLocation(String location) { this.location = location; }

    public void setTableType(String tableType) { this.tableType = tableType; }

    public void setSerialLib(String serialLib) { this.serialLib = serialLib; }

    public void setParameters(String parameters) { this.parameters = parameters; }

    public void setColumnComments(List<String> columnComments) { this.columnComments = columnComments; }
}
