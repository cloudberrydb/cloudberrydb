package cn.hashdata.dlagent.plugins.iceberg.utilities;

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

import cn.hashdata.dlagent.api.error.DlRuntimeException;
import cn.hashdata.dlagent.api.error.UnsupportedTypeException;
import cn.hashdata.dlagent.api.io.DataType;
import cn.hashdata.dlagent.api.model.Metadata;
import cn.hashdata.dlagent.api.model.RequestContext;
import cn.hashdata.dlagent.api.utilities.ColumnDescriptor;
import cn.hashdata.dlagent.api.utilities.EnumGpdbType;
import cn.hashdata.dlagent.api.utilities.Utilities;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.Table;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.stream.Collectors;

/**
 * Class containing helper functions connecting
 * and interacting with Iceberg.
 */
@Component
public class IcebergUtilities {

    /**
     * Checks if iceberg type is supported, and if so return its matching GPDB
     * type. Unsupported types will result in an exception. <br>
     * The supported mappings are:
     * <ul>
     * <li>{@code int -> int4}</li>
     * <li>{@code long -> int8}</li>
     * <li>{@code boolean -> bool}</li>
     * <li>{@code float -> float4}</li>
     * <li>{@code double -> float8}</li>
     * <li>{@code string -> text}</li>
     * <li>{@code uuid -> uuid}</li>
     * <li>{@code binary -> bytea}</li>
     * <li>{@code time -> time}</li>
     * <li>{@code timestamp -> timestamp}</li>
     * <li>{@code date -> date}</li>
     * <li>{@code decimal(precision, scale) -> numeric(precision, scale)}</li>
     * <li>{@code list<dataType> -> text}</li>
     * <li>{@code map<keyDataType, valueDataType> -> text}</li>
     * <li>{@code struct<field1:dataType,...,fieldN:dataType> -> text}</li>
     * </ul>
     *
     * @param icebergColumn iceberg column schema
     * @return field with mapped GPDB type and modifiers
     * @throws UnsupportedTypeException if the column type is not supported
     * @see EnumIcebergToGpdbType
     */
    public Metadata.Field mapIcebergType(Types.NestedField icebergColumn) throws UnsupportedTypeException {
        String fieldName = icebergColumn.name();
        String icebergType = EnumIcebergToGpdbType.getFullIcebergTypeName(icebergColumn); // Type name and modifiers if any
        String icebergTypeName; // Type name
        String[] modifiers = null; // Modifiers
        EnumIcebergToGpdbType icebergToGpdbType = EnumIcebergToGpdbType.getIcebergToGpdbType(icebergType);
        EnumGpdbType gpdbType = icebergToGpdbType.getGpdbType();

        if (icebergToGpdbType.getSplitExpression() != null) {
            String[] tokens = icebergType.split(icebergToGpdbType.getSplitExpression());
            icebergTypeName = tokens[0];
            if (gpdbType.getModifiersNum() > 0) {
                modifiers = Arrays.copyOfRange(tokens, 1, tokens.length);
                if (modifiers.length != gpdbType.getModifiersNum()) {
                    throw new UnsupportedTypeException(
                            "GPDB does not support type " + icebergType
                                    + " (Field " + fieldName + "), "
                                    + "expected number of modifiers: "
                                    + gpdbType.getModifiersNum()
                                    + ", actual number of modifiers: "
                                    + modifiers.length);
                }
                if (!Utilities.verifyIntegerModifiers(modifiers)) {
                    throw new UnsupportedTypeException("GPDB does not support type " + icebergType + " (Field " + fieldName + "), modifiers should be integers");
                }
            }
        } else
            icebergTypeName = icebergType;

        return new Metadata.Field(fieldName, gpdbType, icebergToGpdbType.isComplexType(), icebergTypeName, modifiers);
    }

    /**
     * Validates whether given GPDB and Iceberg data types are compatible.
     * If data type could have modifiers, GPDB data type is valid if it hasn't modifiers at all
     * or GPDB's modifiers are greater or equal to Iceberg's modifiers.
     * <p>
     * For example:
     * <p>
     * Iceberg type - numeric(20), GPDB type decimal(21) - valid.
     *
     * @param gpdbDataType   GPDB data type
     * @param gpdbTypeMods   GPDB type modifiers
     * @param icebergType    full Iceberg type, i.e. decimal(10,2)
     * @param gpdbColumnName Iceberg column name
     * @throws UnsupportedTypeException if types are incompatible
     */
    public void validateTypeCompatible(DataType gpdbDataType, Integer[] gpdbTypeMods, String icebergType, String gpdbColumnName) {
        EnumIcebergToGpdbType icebergToGpdbType = EnumIcebergToGpdbType.getIcebergToGpdbType(icebergType);
        EnumGpdbType expectedGpdbType = icebergToGpdbType.getGpdbType();

        if (!expectedGpdbType.getDataType().equals(gpdbDataType)) {
            throw new UnsupportedTypeException("Invalid definition for column " + gpdbColumnName
                    + ": expected GPDB type " + expectedGpdbType.getDataType() +
                    ", actual GPDB type " + gpdbDataType);
        }

        switch (gpdbDataType) {
            case NUMERIC:
                if (gpdbTypeMods != null && gpdbTypeMods.length > 0) {
                    Integer[] icebergTypeModifiers = EnumIcebergToGpdbType
                            .extractModifiers(icebergType);
                    for (int i = 0; i < icebergTypeModifiers.length; i++) {
                        if (gpdbTypeMods[i] < icebergTypeModifiers[i])
                            throw new UnsupportedTypeException(
                                    "Invalid definition for column " + gpdbColumnName
                                            + ": modifiers are not compatible, "
                                            + Arrays.toString(icebergTypeModifiers) + ", "
                                            + Arrays.toString(gpdbTypeMods));
                    }
                }
                break;
        }
    }

    public TableIdentifier getIcebergTableIdentifier(String tableName) {
        // If database not been specified in property, use default
        if (!tableName.contains(".")) {
            return TableIdentifier.of("default", tableName);
        }
        return TableIdentifier.parse(tableName);
    }

    /**
     * Compose Iceberg catalog properties from Hadoop Configuration.
     */
    public Map<String, String> composeCatalogProperties(Configuration configuration) {
        Map<String, String> props = new HashMap<>();
        List<String> configKeys = new ArrayList<>(Arrays.asList(
                CatalogProperties.FILE_IO_IMPL, CatalogProperties.IO_MANIFEST_CACHE_ENABLED,
                CatalogProperties.IO_MANIFEST_CACHE_EXPIRATION_INTERVAL_MS,
                CatalogProperties.IO_MANIFEST_CACHE_MAX_TOTAL_BYTES,
                CatalogProperties.IO_MANIFEST_CACHE_MAX_CONTENT_LENGTH));

        for (String key : configKeys) {
            String val = configuration.get("iceberg." + key);
            if (val != null) {
                props.put(key, val);
            }
        }

        if (!props.containsKey(CatalogProperties.FILE_IO_IMPL)) {
            // Manifest caching only enabled if "io-impl" is specified. Default to HadoopFileIO
            // if non-existent.
            props.put(CatalogProperties.FILE_IO_IMPL, HadoopFileIO.class.getName());
        }

        return props;
    }

    /**
     * Checks that iceberg fields and partitions match the Greenplum schema.
     * Throws an exception if:
     * - A Greenplum column does not match any columns or partitions on the
     * Iceberg table definition
     * - The iceberg fields types do not match the Greenplum fields.
     *
     * @param schema the iceberg table
     */
    public void verifySchema(Schema schema, RequestContext context) {
        List<Types.NestedField> icebergColumns = schema.columns();
        Map<String, Types.NestedField> columnNameToFieldSchema = icebergColumns.stream()
                        .collect(Collectors.toMap(Types.NestedField::name, fieldSchema -> fieldSchema));

        Types.NestedField fieldSchema;
        for (ColumnDescriptor cd : context.getTupleDescription()) {
            if ((fieldSchema = columnNameToFieldSchema.get(cd.columnName())) == null &&
                    (fieldSchema = columnNameToFieldSchema.get(cd.columnName().toLowerCase())) == null) {
                throw new DlRuntimeException(
                        String.format("column '%s' does not exist in the Iceberg schema",
                                cd.columnName()),
                        "Ensure the column exists and check the name spelling and case."
                );
            }

            validateTypeCompatible(
                    cd.getDataType(),
                    cd.columnTypeModifiers(),
                    EnumIcebergToGpdbType.getFullIcebergTypeName(fieldSchema),
                    cd.columnName());
        }
    }
}
