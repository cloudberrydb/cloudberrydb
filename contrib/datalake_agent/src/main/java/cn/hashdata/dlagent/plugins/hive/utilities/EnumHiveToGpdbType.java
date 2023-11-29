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

package cn.hashdata.dlagent.plugins.hive.utilities;

import cn.hashdata.dlagent.api.error.UnsupportedTypeException;
import cn.hashdata.dlagent.api.io.DataType;
import cn.hashdata.dlagent.api.utilities.EnumGpdbType;

import java.util.Comparator;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Hive types, which are supported by plugin, mapped to GPDB's types
 *
 * @see EnumGpdbType
 */
public enum EnumHiveToGpdbType {

    TinyintType("tinyint", EnumGpdbType.Int2Type, (byte) 1),
    SmallintType("smallint", EnumGpdbType.Int2Type, (byte) 2),
    IntType("int", EnumGpdbType.Int4Type),
    BigintType("bigint", EnumGpdbType.Int8Type),
    BooleanType("boolean", EnumGpdbType.BoolType),
    FloatType("float", EnumGpdbType.Float4Type),
    DoubleType("double", EnumGpdbType.Float8Type),
    StringType("string", EnumGpdbType.TextType),
    BinaryType("binary", EnumGpdbType.ByteaType, true),
    TimestampType("timestamp", EnumGpdbType.TimestampType),
    DateType("date", EnumGpdbType.DateType),
    DecimalType("decimal", EnumGpdbType.NumericType, "[(,)]"),
    VarcharType("varchar", EnumGpdbType.VarcharType, "[(,)]"),
    CharType("char", EnumGpdbType.BpcharType, "[(,)]"),
    ArrayType("array", EnumGpdbType.TextType, "[<,>]", true),
    MapType("map", EnumGpdbType.TextType, "[<,>]", true),
    StructType("struct", EnumGpdbType.TextType, "[<,>]", true),
    UnionType("uniontype", EnumGpdbType.TextType, "[<,>]", true);

    private String typeName;
    private EnumGpdbType gpdbType;
    private String splitExpression;
    private byte size;
    private boolean isComplexType;

    EnumHiveToGpdbType(String typeName, EnumGpdbType gpdbType) {
        this.typeName = typeName;
        this.gpdbType = gpdbType;
    }

    EnumHiveToGpdbType(String typeName, EnumGpdbType gpdbType, byte size) {
        this(typeName, gpdbType);
        this.size = size;
    }

    EnumHiveToGpdbType(String typeName, EnumGpdbType gpdbType, boolean isComplexType) {
        this(typeName, gpdbType);
        this.isComplexType = isComplexType;
    }

    EnumHiveToGpdbType(String typeName, EnumGpdbType gpdbType, String splitExpression) {
        this(typeName, gpdbType);
        this.splitExpression = splitExpression;
    }

    EnumHiveToGpdbType(String typeName, EnumGpdbType gpdbType, String splitExpression, boolean isComplexType) {
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
     * Returns Hive to GPDB type mapping entry for given Hive type
     *
     * @param hiveType full Hive type with modifiers, for example - decimal(10, 0), char(5), binary, array&lt;string&gt;, map&lt;string,float&gt; etc
     * @return corresponding Hive to GPDB type mapping entry
     * @throws UnsupportedTypeException if there is no corresponding GPDB type
     */
    public static EnumHiveToGpdbType getHiveToGpdbType(String hiveType) {
        for (EnumHiveToGpdbType t : values()) {
            String hiveTypeName = hiveType;
            String splitExpression = t.getSplitExpression();
            if (splitExpression != null) {
                String[] tokens = hiveType.split(splitExpression);
                hiveTypeName = tokens[0];
            }

            if (t.getTypeName().toLowerCase().equals(hiveTypeName.toLowerCase())) {
                return t;
            }
        }
        throw new UnsupportedTypeException("Unable to map Hive's type: "
                + hiveType + " to GPDB's type");
    }


    /**
     * @param dataType Gpdb data type
     * @return compatible Hive type to given Gpdb type, if there are more than one compatible types, it returns one with bigger size
     * @throws UnsupportedTypeException if there is no corresponding Hive type for given Gpdb type
     */
    public static EnumHiveToGpdbType getCompatibleHiveToGpdbType(DataType dataType) {

        SortedSet<EnumHiveToGpdbType> types = new TreeSet<EnumHiveToGpdbType>(
                new Comparator<EnumHiveToGpdbType>() {
                    public int compare(EnumHiveToGpdbType a,
                                       EnumHiveToGpdbType b) {
                        return Byte.compare(a.getSize(), b.getSize());
                    }
                });

        for (EnumHiveToGpdbType t : values()) {
            if (t.getGpdbType().getDataType().equals(dataType)) {
                types.add(t);
            }
        }

        if (types.size() == 0)
            throw new UnsupportedTypeException("Unable to find compatible Hive type for given GPDB's type: " + dataType);

        return types.last();
    }

    /**
     * @param hiveToGpdbType EnumHiveToGpdbType enum
     * @param modifiers      Array of Modifiers
     * @return full Hive type name including modifiers. eg: varchar(3)
     * This function is used for datatypes with modifier information
     * such as varchar, char, decimal, etc.
     */
    public static String getFullHiveTypeName(EnumHiveToGpdbType hiveToGpdbType, Integer[] modifiers) {
        hiveToGpdbType.getTypeName();
        if (modifiers != null && modifiers.length > 0) {
            String modExpression = hiveToGpdbType.getSplitExpression();
            StringBuilder fullType = new StringBuilder(hiveToGpdbType.typeName);
            Character start = modExpression.charAt(1);
            Character separator = modExpression.charAt(2);
            Character end = modExpression.charAt(modExpression.length() - 2);
            fullType.append(start);
            int index = 0;
            for (Integer modifier : modifiers) {
                if (index++ > 0) {
                    fullType.append(separator);
                }
                fullType.append(modifier);
            }
            fullType.append(end);
            return fullType.toString();
        } else {
            return hiveToGpdbType.getTypeName();
        }
    }

    /**
     * @param hiveType full Hive data type, i.e. varchar(10) etc
     * @return array of type modifiers
     * @throws UnsupportedTypeException if there is no such Hive type supported
     */
    public static Integer[] extractModifiers(String hiveType) {
        Integer[] result = null;
        for (EnumHiveToGpdbType t : values()) {
            String hiveTypeName = hiveType;
            String splitExpression = t.getSplitExpression();
            if (splitExpression != null) {
                String[] tokens = hiveType.split(splitExpression);
                hiveTypeName = tokens[0];
                result = new Integer[tokens.length - 1];
                for (int i = 0; i < tokens.length - 1; i++)
                    result[i] = Integer.parseInt(tokens[i + 1]);
            }
            if (t.getTypeName().toLowerCase()
                    .equals(hiveTypeName.toLowerCase())) {
                return result;
            }
        }
        throw new UnsupportedTypeException("Unable to map Hive's type: "
                + hiveType + " to GPDB's type");
    }

    /**
     * This field is needed to find compatible Hive type when more than one Hive type mapped to GPDB type
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