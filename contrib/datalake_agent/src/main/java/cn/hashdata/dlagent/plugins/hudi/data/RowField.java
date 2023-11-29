package cn.hashdata.dlagent.plugins.hudi.data;

import org.apache.hudi.internal.schema.Type;

public class RowField {
    private final String name;
    private Type.TypeID dataType;

    public RowField(String name, Type.TypeID dataType) {
        this.name = name;
        this.dataType = dataType;
    }

    public String getName() {
        return name;
    }

    public Type.TypeID getDataType() {
        return dataType;
    }
  }
