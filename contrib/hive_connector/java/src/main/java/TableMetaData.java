import org.apache.hadoop.hive.metastore.api.Partition;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class TableMetaData {
    private List<String> partKeys;
    private List<String> partKeyTypes;
    private List<Partition> partitions;
    private List<String> fields;
    private String field;
    private String format;
    private boolean isPartitionTable;
    private String location;
    private String tableType;
    private String serialLib;
    private String parameters;
    private boolean isTransactionalTable;

    public Object[] getPartKeys() {
        return this.partKeys.toArray();
    }

    public Object[] getPartKeyTypes() {
        return this.partKeyTypes.toArray();
    }

    public Object[] getPartKeyValues(int index) {
        HashSet<String> values = new HashSet<String>();
        for (Partition partition : partitions) {
            values.add(partition.getValues().get(index));
        }

        return values.toArray();
    }

    public Object[] getLocations() {
        List<String> locations = new ArrayList<>();

        for(Partition partition : partitions) {
            locations.add(partition.getSd().getLocation());
        }

        return locations.toArray();
    }

    public Object[] getPartValues(int index) {
        return partitions.get(index).getValues().toArray();
    }

    public Object[] getPartFields() {
        return this.fields.toArray();
    }

    public Object getField() { return this.field;}

    public Object getFormat() {return this.format;}

    public int isPartitionTable() { return this.isPartitionTable ? 1 : 0; }

    public int isTransactionalTable() { return this.isTransactionalTable ? 1 : 0; }

    public Object getTableType() { return this.tableType; }

    public Object getSerialLib() { return this.serialLib; }

    public Object getParameters() { return this.parameters; }

    public int getPartitionNumber() { return this.partitions.size(); }

    public Object getLocation() {return this.location; }

    public void setPartKeys(List<String> partKeys) {
        this.partKeys = partKeys;
    }

    public void setPartKeyTypes(List<String> partKeyTypes) {
        this.partKeyTypes = partKeyTypes;
    }

    public void setPartitions(List<Partition> partitions) {
        this.partitions = partitions;
    }

    public void setPartFields(List<String> fields) { this.fields = fields; }

    public void setField(String field) { this.field = field; }

    public void setFormat(String format) { this.format = format; }

    public void setPartitionTable(boolean isPartitionTable) { this.isPartitionTable = isPartitionTable; }

    public void setIsTransactionalTable(boolean isTransactionalTable) { this.isTransactionalTable = isTransactionalTable; }

    public void setLocation(String location) { this.location = location; }

    public void setTableType(String tableType) { this.tableType = tableType; }

    public void setSerialLib(String serialLib) { this.serialLib = serialLib; }

    public void setParameters(String parameters) { this.parameters = parameters; }
}
