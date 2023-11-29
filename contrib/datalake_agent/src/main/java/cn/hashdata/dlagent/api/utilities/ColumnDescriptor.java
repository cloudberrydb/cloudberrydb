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


import org.apache.commons.lang.ArrayUtils;
import cn.hashdata.dlagent.api.io.DataType;

/**
 * ColumnDescriptor describes one column in gpdb database.
 * Currently it means a name, a type id (GPDB OID), a type name and column index.
 */
public class ColumnDescriptor {

    private final DataType dataType;
    private final int dbColumnTypeCode;
    private final String dbColumnName;
    private final String dbColumnTypeName;
    private final int dbColumnIndex;
    private final Integer[] dbColumnTypeModifiers;
    private boolean isProjected;

    /**
     * Reserved word for a table record key.
     * A field with this name will be treated as record key.
     */
    public static final String RECORD_KEY_NAME = "recordkey";

    /**
     * Constructs a ColumnDescriptor.
     *
     * @param name     column name
     * @param typecode OID
     * @param index    column index
     * @param typename type name
     * @param typemods type modifiers
     */
    public ColumnDescriptor(String name,
                            int typecode,
                            int index,
                            String typename,
                            Integer[] typemods) {
        this(name, typecode, index, typename, typemods, true);
    }

    /**
     * Constructs a ColumnDescriptor.
     *
     * @param name     column name
     * @param typecode OID
     * @param index    column index
     * @param typename type name
     * @param typemods type modifiers
     * @param isProj   does the column need to be projected
     */
    public ColumnDescriptor(String name,
                            int typecode,
                            int index,
                            String typename,
                            Integer[] typemods,
                            boolean isProj) {
        dbColumnTypeCode = typecode;
        dbColumnTypeName = typename;
        dbColumnName = name;
        dbColumnIndex = index;
        dbColumnTypeModifiers = typemods;
        isProjected = isProj;
        dataType = DataType.get(dbColumnTypeCode);
    }

    /**
     * Constructs a copy of ColumnDescriptor.
     *
     * @param copy the ColumnDescriptor to copy
     */
    public ColumnDescriptor(ColumnDescriptor copy) {
        this(copy.dbColumnName,
                copy.dbColumnTypeCode,
                copy.dbColumnIndex,
                copy.dbColumnTypeName,
                ArrayUtils.isNotEmpty(copy.dbColumnTypeModifiers) ? new Integer[copy.dbColumnTypeModifiers.length] : null,
                copy.isProjected);

        if (ArrayUtils.isNotEmpty(copy.dbColumnTypeModifiers)) {
            System.arraycopy(copy.dbColumnTypeModifiers, 0,
                    this.dbColumnTypeModifiers, 0,
                    copy.dbColumnTypeModifiers.length);
        }
    }

    /**
     * Returns the column name
     *
     * @return the column name
     */
    public String columnName() {
        return dbColumnName;
    }

    /**
     * Returns the column type code
     *
     * @return the column type code
     */
    public int columnTypeCode() {
        return dbColumnTypeCode;
    }

    /**
     * Returns the column index
     *
     * @return the column index
     */
    public int columnIndex() {
        return dbColumnIndex;
    }

    /**
     * Returns the column type name
     *
     * @return the column type name
     */
    public String columnTypeName() {
        return dbColumnTypeName;
    }

    /**
     * Returns the column type modifiers
     *
     * @return the column type modifiers
     */
    public Integer[] columnTypeModifiers() {
        return dbColumnTypeModifiers;
    }

    /**
     * Returns the {@link DataType} for the column
     *
     * @return the {@link DataType} for the column
     */
    public DataType getDataType() {
        return dataType;
    }

    /**
     * Returns <tt>true</tt> if {@link #dbColumnName} is a {@link #RECORD_KEY_NAME}.
     *
     * @return whether column is a record key column
     */
    public boolean isKeyColumn() {
        return RECORD_KEY_NAME.equalsIgnoreCase(dbColumnName);
    }

    /**
     * Returns true if the column is projected, false otherwise
     *
     * @return true if the column is projected, false otherwise
     */
    public boolean isProjected() {
        return isProjected;
    }

    /**
     * Sets the value of the column projection
     *
     * @param projected true if the column is projected, false otherwise
     */
    public void setProjected(boolean projected) {
        isProjected = projected;
    }

    @Override
    public String toString() {
        return "ColumnDescriptor [dbColumnTypeCode=" + dbColumnTypeCode
                + ", dbColumnName=" + dbColumnName
                + ", dbColumnTypeName=" + dbColumnTypeName
                + ", dbColumnIndex=" + dbColumnIndex
                + ", dbColumnTypeModifiers=" + dbColumnTypeModifiers
                + ", isProjected=" + isProjected + "]";
    }
}
