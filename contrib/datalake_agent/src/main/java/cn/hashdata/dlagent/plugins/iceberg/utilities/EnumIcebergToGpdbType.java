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

package cn.hashdata.dlagent.plugins.iceberg.utilities;

import cn.hashdata.dlagent.api.error.UnsupportedTypeException;
import cn.hashdata.dlagent.api.io.DataType;
import cn.hashdata.dlagent.api.utilities.EnumGpdbType;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.Comparator;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Iceberg types, which are supported by plugin, mapped to GPDB's types
 *
 * @see EnumGpdbType
 */
public enum EnumIcebergToGpdbType {

    IntegerType("integer", EnumGpdbType.Int4Type),
    LongType("long", EnumGpdbType.Int8Type),
    BooleanType("boolean", EnumGpdbType.BoolType),
    FloatType("float", EnumGpdbType.Float4Type),
    DoubleType("double", EnumGpdbType.Float8Type),
    StringType("string", EnumGpdbType.TextType),
    UuidType("uuid", EnumGpdbType.UuidType),
    BinaryType("binary", EnumGpdbType.ByteaType, true),
    TimeType("time", EnumGpdbType.TimeType),
    TimestampType("timestamp", EnumGpdbType.TimestampType),
    TimestamptzType("timestamptz", EnumGpdbType.TimestamptzType),
    DateType("date", EnumGpdbType.DateType),
    DecimalType("decimal", EnumGpdbType.NumericType, "[(,)]"),
    ArrayType("list", EnumGpdbType.TextType, "[<,>]", true),
    MapType("map", EnumGpdbType.TextType, "[<,>]", true),
    StructType("struct", EnumGpdbType.TextType, "[<,>]", true);

    private String typeName;
    private EnumGpdbType gpdbType;
    private String splitExpression;
    private byte size;
    private boolean isComplexType;

    EnumIcebergToGpdbType(String typeName, EnumGpdbType gpdbType) {
        this.typeName = typeName;
        this.gpdbType = gpdbType;
    }

    EnumIcebergToGpdbType(String typeName, EnumGpdbType gpdbType, byte size) {
        this(typeName, gpdbType);
        this.size = size;
    }

    EnumIcebergToGpdbType(String typeName, EnumGpdbType gpdbType, boolean isComplexType) {
        this(typeName, gpdbType);
        this.isComplexType = isComplexType;
    }

    EnumIcebergToGpdbType(String typeName, EnumGpdbType gpdbType, String splitExpression) {
        this(typeName, gpdbType);
        this.splitExpression = splitExpression;
    }

    EnumIcebergToGpdbType(String typeName, EnumGpdbType gpdbType, String splitExpression, boolean isComplexType) {
        this(typeName, gpdbType, splitExpression);
        this.isComplexType = isComplexType;
    }

    /**
     * @return name of type
     */
    public String getTypeName() {
        return this.typeName;
    }

    /**
     * @return corresponding GPDB type
     */
    public EnumGpdbType getGpdbType() {
        return this.gpdbType;
    }

    /**
     * @return split by expression
     */
    public String getSplitExpression() {
        return this.splitExpression;
    }

    /**
     * Returns Iceberg to GPDB type mapping entry for given Iceberg type
     *
     * @param icebergType full Iceberg type with modifiers, for example - decimal(10, 0), binary, array&lt;string&gt;, map&lt;string,float&gt; etc
     * @return corresponding Iceberg to GPDB type mapping entry
     * @throws UnsupportedTypeException if there is no corresponding GPDB type
     */
    public static EnumIcebergToGpdbType getIcebergToGpdbType(String icebergType) {
        for (EnumIcebergToGpdbType t : values()) {
            String icebergTypeName = icebergType;
            String splitExpression = t.getSplitExpression();
            if (splitExpression != null) {
                String[] tokens = icebergType.split(splitExpression);
                icebergTypeName = tokens[0];
            }

            if (t.getTypeName().toLowerCase().equals(icebergTypeName.toLowerCase())) {
                return t;
            }
        }
        throw new UnsupportedTypeException("Unable to map Iceberg's type: "
                + icebergType + " to GPDB's type");
    }


    /**
     * @param dataType Gpdb data type
     * @return compatible Iceberg type to given Gpdb type, if there are more than one compatible types, it returns one with bigger size
     * @throws UnsupportedTypeException if there is no corresponding Iceberg type for given Gpdb type
     */
    public static EnumIcebergToGpdbType getCompatibleIcebergToGpdbType(DataType dataType) {

        SortedSet<EnumIcebergToGpdbType> types = new TreeSet<EnumIcebergToGpdbType>(
                new Comparator<EnumIcebergToGpdbType>() {
                    public int compare(EnumIcebergToGpdbType a,
                                       EnumIcebergToGpdbType b) {
                        return Byte.compare(a.getSize(), b.getSize());
                    }
                });

        for (EnumIcebergToGpdbType t : values()) {
            if (t.getGpdbType().getDataType().equals(dataType)) {
                types.add(t);
            }
        }

        if (types.size() == 0)
            throw new UnsupportedTypeException("Unable to find compatible Iceberg type for given GPDB's type: " + dataType);

        return types.last();
    }

    public static String getFullIcebergTypeName(Types.NestedField icebergColumn) {
        Type icebergType = icebergColumn.type();

        switch (icebergType.typeId()) {
            case INTEGER:
            case LONG:
            case BOOLEAN:
            case FLOAT:
            case DOUBLE:
            case STRING:
            case UUID:
            case BINARY:
            case TIME:
            case DATE:
                return icebergType.typeId().name();
            case TIMESTAMP:
                Types.TimestampType timestamp = (Types.TimestampType) icebergType;
                if (timestamp.shouldAdjustToUTC()) {
                    return "timestamptz";
                } else {
                    return "timestamp";
                }
            case DECIMAL:
                Types.DecimalType decimal = (Types.DecimalType) icebergType;
                return "decimal(" + decimal.precision() + "," + decimal.scale() +")";
            default:
                throw new UnsupportedTypeException("GPDB does not support type " + icebergType + " (Field " + icebergColumn.name() + ")");
        }
    }

    /**
     * @param icebergType full Iceberg data type, i.e. varchar(10) etc
     * @return array of type modifiers
     * @throws UnsupportedTypeException if there is no such Iceberg type supported
     */
    public static Integer[] extractModifiers(String icebergType) {
        Integer[] result = null;
        for (EnumIcebergToGpdbType t : values()) {
            String icebergTypeName = icebergType;
            String splitExpression = t.getSplitExpression();
            if (splitExpression != null) {
                String[] tokens = icebergType.split(splitExpression);
                icebergTypeName = tokens[0];
                result = new Integer[tokens.length - 1];
                for (int i = 0; i < tokens.length - 1; i++)
                    result[i] = Integer.parseInt(tokens[i + 1]);
            }
            if (t.getTypeName().toLowerCase()
                    .equals(icebergTypeName.toLowerCase())) {
                return result;
            }
        }
        throw new UnsupportedTypeException("Unable to map Iceberg's type: "
                + icebergType + " to GPDB's type");
    }

    /**
     * This field is needed to find compatible Iceberg type when more than one Iceberg type mapped to GPDB type
     *
     * @return size of this type in bytes or 0
     */
    public byte getSize() {
        return size;
    }

    public boolean isComplexType() {
        return isComplexType;
    }

    public void setComplexType(boolean isComplexType) {
        this.isComplexType = isComplexType;
    }

}