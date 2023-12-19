package cn.hashdata.dlagent.plugins.hudi.utilities;

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
import cn.hashdata.dlagent.api.security.SecureLogin;
import cn.hashdata.dlagent.api.utilities.ColumnDescriptor;
import cn.hashdata.dlagent.api.utilities.EnumGpdbType;
import cn.hashdata.dlagent.api.utilities.Utilities;
import cn.hashdata.dlagent.plugins.hive.utilities.DlCachedClientPool;
import cn.hashdata.dlagent.plugins.hudi.HudiFsBackedTableMetadata;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.Types;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.Collections;
import java.util.stream.Collectors;

/**
 * Class containing helper functions connecting
 * and interacting with Hudi.
 */
@Component
public class HudiUtilities {
    private static final Logger LOG = LoggerFactory.getLogger(HudiUtilities.class);
    /**
     * Checks if hudi type is supported, and if so return its matching GPDB
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
     * <li>{@code fixed -> bytea}</li>
     * <li>{@code array<dataType> -> text}</li>
     * <li>{@code map<keyDataType, valueDataType> -> text}</li>
     * <li>{@code struct<field1:dataType,...,fieldN:dataType> -> text}</li>
     * </ul>
     *
     * @param hudiColumn hudi column schema
     * @return field with mapped GPDB type and modifiers
     * @throws UnsupportedTypeException if the column type is not supported
     * @see EnumHudiToGpdbType
     */
    public Metadata.Field mapHudiType(Types.Field hudiColumn) throws UnsupportedTypeException {
        String fieldName = hudiColumn.name();
        String hudiType = EnumHudiToGpdbType.getFullHudiTypeName(hudiColumn); // Type name and modifiers if any
        String hudiTypeName; // Type name
        String[] modifiers = null; // Modifiers
        EnumHudiToGpdbType hudiToGpdbType = EnumHudiToGpdbType.getHudiToGpdbType(hudiType);
        EnumGpdbType gpdbType = hudiToGpdbType.getGpdbType();

        if (hudiToGpdbType.getSplitExpression() != null) {
            String[] tokens = hudiType.split(hudiToGpdbType.getSplitExpression());
            hudiTypeName = tokens[0];
            if (gpdbType.getModifiersNum() > 0) {
                modifiers = Arrays.copyOfRange(tokens, 1, tokens.length);
                if (modifiers.length != gpdbType.getModifiersNum()) {
                    throw new UnsupportedTypeException(
                            "GPDB does not support type " + hudiType
                                    + " (Field " + fieldName + "), "
                                    + "expected number of modifiers: "
                                    + gpdbType.getModifiersNum()
                                    + ", actual number of modifiers: "
                                    + modifiers.length);
                }
                if (!Utilities.verifyIntegerModifiers(modifiers)) {
                    throw new UnsupportedTypeException("GPDB does not support type " + hudiType + " (Field " + fieldName + "), modifiers should be integers");
                }
            }
        } else
            hudiTypeName = hudiType;

        return new Metadata.Field(fieldName, gpdbType, hudiToGpdbType.isComplexType(), hudiTypeName, modifiers);
    }

    /**
     * Validates whether given GPDB and Hudi data types are compatible.
     * If data type could have modifiers, GPDB data type is valid if it hasn't modifiers at all
     * or GPDB's modifiers are greater or equal to Hudi's modifiers.
     * <p>
     * For example:
     * <p>
     * Hudi type - numeric(20), GPDB type decimal(21) - valid.
     *
     * @param gpdbDataType   GPDB data type
     * @param gpdbTypeMods   GPDB type modifiers
     * @param hudiType    full Hudi type, i.e. decimal(10,2)
     * @param gpdbColumnName Hudi column name
     * @throws UnsupportedTypeException if types are incompatible
     */
    public void validateTypeCompatible(DataType gpdbDataType, Integer[] gpdbTypeMods, String hudiType, String gpdbColumnName) {
        EnumHudiToGpdbType hudiToGpdbType = EnumHudiToGpdbType.getHudiToGpdbType(hudiType);
        EnumGpdbType expectedGpdbType = hudiToGpdbType.getGpdbType();

        if (!expectedGpdbType.getDataType().equals(gpdbDataType)) {
            throw new UnsupportedTypeException("Invalid definition for column " + gpdbColumnName
                    + ": expected GPDB type " + expectedGpdbType.getDataType() +
                    ", actual GPDB type " + gpdbDataType);
        }

        switch (gpdbDataType) {
            case NUMERIC:
                if (gpdbTypeMods != null && gpdbTypeMods.length > 0) {
                    Integer[] hudiTypeModifiers = EnumHudiToGpdbType
                            .extractModifiers(hudiType);
                    for (int i = 0; i < hudiTypeModifiers.length; i++) {
                        if (gpdbTypeMods[i] < hudiTypeModifiers[i])
                            throw new UnsupportedTypeException(
                                    "Invalid definition for column " + gpdbColumnName
                                            + ": modifiers are not compatible, "
                                            + Arrays.toString(hudiTypeModifiers) + ", "
                                            + Arrays.toString(gpdbTypeMods));
                    }
                }
                break;
        }
    }

    /**
     * Compose Hudi catalog properties from Hadoop Configuration.
     */
    public Map<String, String> composeCatalogProperties(RequestContext context) {
        Map<String, String> props = new HashMap<>();

        props.put("hoodie.datasource.query.type", context.getQueryType());
        props.put("path", context.getPath());

        return props;
    }

    /**
     * Checks that hudi fields and partitions match the Greenplum schema.
     * Throws an exception if:
     * - A Greenplum column does not match any columns or partitions on the
     * Hudi table definition
     * - The hudi fields types do not match the Greenplum fields.
     *
     * @param schema the schema of hudi table
     */
    public void verifySchema(InternalSchema schema, RequestContext context) {
        List<Types.Field> hudiColumns = schema.columns();
        Map<String, Types.Field> columnNameToFieldSchema = hudiColumns.stream()
                        .collect(Collectors.toMap(Types.Field::name, fieldSchema -> fieldSchema));

        Types.Field fieldSchema;
        for (ColumnDescriptor cd : context.getTupleDescription()) {
            if ((fieldSchema = columnNameToFieldSchema.get(cd.columnName())) == null &&
                    (fieldSchema = columnNameToFieldSchema.get(cd.columnName().toLowerCase())) == null) {
                throw new DlRuntimeException(
                        String.format("column '%s' does not exist in the Hudi schema",
                                cd.columnName()),
                        "Ensure the column exists and check the name spelling and case."
                );
            }

            validateTypeCompatible(
                    cd.getDataType(),
                    cd.columnTypeModifiers(),
                    EnumHudiToGpdbType.getFullHudiTypeName(fieldSchema),
                    cd.columnName());
        }
    }

    /**
     * Returns whether the hoodie table exists under given path {@code basePath}.
     */
    public static boolean tableExists(String basePath, Configuration hadoopConf) {
        // Hadoop FileSystem
        FileSystem fs = FSUtils.getFs(basePath, hadoopConf);
        try {
            return fs.exists(new Path(basePath, HoodieTableMetaClient.METAFOLDER_NAME))
                    && fs.exists(new Path(new Path(basePath, HoodieTableMetaClient.METAFOLDER_NAME),
                    HoodieTableConfig.HOODIE_PROPERTIES_FILE));
        } catch (IOException e) {
            throw new HoodieException("Error while checking whether table exists under path:" + basePath, e);
        }
    }

    public static List<String> getAllPartitionPaths(HoodieEngineContext engineContext, HoodieMetadataConfig metadataConfig,
                                                    String basePathStr, RequestContext context, SecureLogin secureLogin) {
        try (HoodieTableMetadata tableMetadata = create(engineContext, metadataConfig, basePathStr,
                FileSystemViewStorageConfig.SPILLABLE_DIR.defaultValue(), false, context, secureLogin)) {
            return tableMetadata.getAllPartitionPaths();
        } catch (Exception e) {
            throw new HoodieException("Error fetching partition paths from metadata table", e);
        }
    }

    public static Map<String, FileStatus[]> getFilesInPartitions(HoodieEngineContext engineContext,
                                                                 HoodieMetadataConfig metadataConfig,
                                                                 String basePathStr,
                                                                 String[] partitionPaths,
                                                                 RequestContext context,
                                                                 SecureLogin secureLogin) {
        try (HoodieTableMetadata tableMetadata = create(engineContext, metadataConfig, basePathStr,
                FileSystemViewStorageConfig.SPILLABLE_DIR.defaultValue(), true, context, secureLogin)) {
            return tableMetadata.getAllFilesInPartitions(Arrays.asList(partitionPaths));
        } catch (Exception e) {
            throw new HoodieException("Error get files in partitions: " + String.join(",", partitionPaths), e);
        }
    }

    static HoodieTableMetadata create(HoodieEngineContext engineContext, HoodieMetadataConfig metadataConfig, String datasetBasePath,
                                      String spillableMapPath, Boolean reuse, RequestContext context, SecureLogin secureLogin) {
        if (metadataConfig.enabled()) {
            return createHoodieBackedTableMetadata(engineContext, metadataConfig, datasetBasePath, spillableMapPath, reuse);
        } else {
            return createFSBackedTableMetadata(engineContext, metadataConfig, datasetBasePath, context, secureLogin);
        }
    }

    static HudiFsBackedTableMetadata createFSBackedTableMetadata(HoodieEngineContext engineContext,
                                                                 HoodieMetadataConfig metadataConfig,
                                                                 String datasetBasePath,
                                                                 RequestContext context,
                                                                 SecureLogin secureLogin) {
        return new HudiFsBackedTableMetadata(engineContext,
                new SerializableConfiguration(engineContext.getHadoopConf()),
                datasetBasePath,
                metadataConfig.shouldAssumeDatePartitioning(),
                secureLogin,
                context);
    }

    static HoodieBackedTableMetadata createHoodieBackedTableMetadata(HoodieEngineContext engineContext,
                                                                     HoodieMetadataConfig metadataConfig,
                                                                     String datasetBasePath,
                                                                     String spillableMapPath,
                                                                     Boolean reuse) {
        return new HoodieBackedTableMetadata(engineContext, metadataConfig, datasetBasePath, spillableMapPath, reuse);
    }

    public static List<String> getAllPartitionPaths(DlCachedClientPool clients,
                                                    boolean isPartitionTable,
                                                    String dbName,
                                                    String tableName) {
        if (!isPartitionTable) {
            return Collections.singletonList("");
        }

        try {
            return clients.run(client -> client.listPartitionNames(dbName, tableName, (short) -1));
        } catch (Exception e) {
            throw new HoodieException("Error fetching partition paths from metadata table", e);
        }
    }
}
