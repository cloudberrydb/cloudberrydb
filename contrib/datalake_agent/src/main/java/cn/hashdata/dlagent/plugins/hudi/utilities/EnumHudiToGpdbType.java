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

package cn.hashdata.dlagent.plugins.hudi.utilities;

import cn.hashdata.dlagent.api.error.UnsupportedTypeException;
import cn.hashdata.dlagent.api.io.DataType;
import cn.hashdata.dlagent.api.utilities.EnumGpdbType;
import org.apache.hudi.internal.schema.Types;
import org.apache.hudi.internal.schema.Type;

import java.util.Comparator;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Hudi types, which are supported by plugin, mapped to GPDB's types
 *
 * @see EnumGpdbType
 */
public enum EnumHudiToGpdbType {

    IntType("int", EnumGpdbType.Int4Type),
    LongType("long", EnumGpdbType.Int8Type),
    BooleanType("boolean", EnumGpdbType.BoolType),
    FloatType("float", EnumGpdbType.Float4Type),
    DoubleType("double", EnumGpdbType.Float8Type),
    StringType("string", EnumGpdbType.TextType),
    UuidType("uuid", EnumGpdbType.UuidType),
    BinaryType("binary", EnumGpdbType.ByteaType, true),
    TimeType("time", EnumGpdbType.TimeType),
    TimestampType("timestamp", EnumGpdbType.TimestampType),
    DateType("date", EnumGpdbType.DateType),
    DecimalType("decimal", EnumGpdbType.NumericType, "[(,)]"),
    FixedType("fixed", EnumGpdbType.ByteaType, "[<,>]",true),
    ArrayType("array", EnumGpdbType.TextType, "[<,>]", true),
    MapType("map", EnumGpdbType.TextType, "[<,>]", true),
    RECORD("record", EnumGpdbType.TextType, "[<,>]", true);

    private String typeName;
    private EnumGpdbType gpdbType;
    private String splitExpression;
    private byte size;
    private boolean isComplexType;

    EnumHudiToGpdbType(String typeName, EnumGpdbType gpdbType) {
        this.typeName = typeName;
        this.gpdbType = gpdbType;
    }

    EnumHudiToGpdbType(String typeName, EnumGpdbType gpdbType, byte size) {
        this(typeName, gpdbType);
        this.size = size;
    }

    EnumHudiToGpdbType(String typeName, EnumGpdbType gpdbType, boolean isComplexType) {
        this(typeName, gpdbType);
        this.isComplexType = isComplexType;
    }

    EnumHudiToGpdbType(String typeName, EnumGpdbType gpdbType, String splitExpression) {
        this(typeName, gpdbType);
        this.splitExpression = splitExpression;
    }

    EnumHudiToGpdbType(String typeName, EnumGpdbType gpdbType, String splitExpression, boolean isComplexType) {
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
     * Returns Hudi to GPDB type mapping entry for given Hudi type
     *
     * @param hudiType full Hudi type with modifiers, for example - decimal(10, 0), binary, array&lt;string&gt;, map&lt;string,float&gt; etc
     * @return corresponding Hudi to GPDB type mapping entry
     * @throws UnsupportedTypeException if there is no corresponding GPDB type
     */
    public static EnumHudiToGpdbType getHudiToGpdbType(String hudiType) {
        for (EnumHudiToGpdbType t : values()) {
            String hudiTypeName = hudiType;
            String splitExpression = t.getSplitExpression();
            if (splitExpression != null) {
                String[] tokens = hudiType.split(splitExpression);
                hudiTypeName = tokens[0];
            }

            if (t.getTypeName().toLowerCase().equals(hudiTypeName.toLowerCase())) {
                return t;
            }
        }
        throw new UnsupportedTypeException("Unable to map Hudi's type: "
                + hudiType + " to GPDB's type");
    }


    /**
     * @param dataType Gpdb data type
     * @return compatible Hudi type to given Gpdb type, if there are more than one compatible types, it returns one with bigger size
     * @throws UnsupportedTypeException if there is no corresponding Hudi type for given Gpdb type
     */
    public static EnumHudiToGpdbType getCompatibleHudiToGpdbType(DataType dataType) {

        SortedSet<EnumHudiToGpdbType> types = new TreeSet<EnumHudiToGpdbType>(
                new Comparator<EnumHudiToGpdbType>() {
                    public int compare(EnumHudiToGpdbType a,
                                       EnumHudiToGpdbType b) {
                        return Byte.compare(a.getSize(), b.getSize());
                    }
                });

        for (EnumHudiToGpdbType t : values()) {
            if (t.getGpdbType().getDataType().equals(dataType)) {
                types.add(t);
            }
        }

        if (types.size() == 0)
            throw new UnsupportedTypeException("Unable to find compatible Hudi type for given GPDB's type: " + dataType);

        return types.last();
    }

    public static String getFullHudiTypeName(Types.Field hudiColumn) {
        Type hudiType = hudiColumn.type();

        switch (hudiType.typeId()) {
            case INT:
            case LONG:
            case BOOLEAN:
            case FLOAT:
            case DOUBLE:
            case STRING:
            case UUID:
            case BINARY:
            case TIME:
            case TIMESTAMP:
            case DATE:
                return hudiType.typeId().name();
            case FIXED:
                Types.FixedType fixed = (Types.FixedType) hudiType;
                return "fixed(" + fixed.getFixedSize() + ")";
            case DECIMAL:
                Types.DecimalType decimal = (Types.DecimalType) hudiType;
                return "decimal(" + decimal.precision() + "," + decimal.scale() +")";
            default:
                throw new UnsupportedTypeException("GPDB does not support type " + hudiType + " (Field " + hudiColumn.name() + ")");
        }
    }

    /**
     * @param hudiType full Hudi data type, i.e. varchar(10) etc
     * @return array of type modifiers
     * @throws UnsupportedTypeException if there is no such Hudi type supported
     */
    public static Integer[] extractModifiers(String hudiType) {
        Integer[] result = null;
        for (EnumHudiToGpdbType t : values()) {
            String hudiTypeName = hudiType;
            String splitExpression = t.getSplitExpression();
            if (splitExpression != null) {
                String[] tokens = hudiType.split(splitExpression);
                hudiTypeName = tokens[0];
                result = new Integer[tokens.length - 1];
                for (int i = 0; i < tokens.length - 1; i++)
                    result[i] = Integer.parseInt(tokens[i + 1]);
            }
            if (t.getTypeName().toLowerCase()
                    .equals(hudiTypeName.toLowerCase())) {
                return result;
            }
        }
        throw new UnsupportedTypeException("Unable to map Hudi's type: "
                + hudiType + " to GPDB's type");
    }

    /**
     * This field is needed to find compatible Hudi type when more than one Hudi type mapped to GPDB type
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