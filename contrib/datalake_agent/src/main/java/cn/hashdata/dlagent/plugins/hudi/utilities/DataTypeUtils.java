/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.hashdata.dlagent.plugins.hudi.utilities;

import cn.hashdata.dlagent.api.error.UnsupportedTypeException;
import cn.hashdata.dlagent.api.io.DataType;
import cn.hashdata.dlagent.api.utilities.ColumnDescriptor;
import cn.hashdata.dlagent.plugins.hudi.data.RowField;
import org.apache.hudi.internal.schema.Type;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DataTypeUtils {
  /**
   * Resolves the partition path string into value obj with given data type.
   */
  public static Object resolvePartition(String partition, Type.TypeID type) {
    if (partition == null) {
      return null;
    }

    switch (type) {
      case STRING:
        return partition;
      case BOOLEAN:
        return Boolean.parseBoolean(partition);
      case INT:
        return Integer.valueOf(partition);
      case LONG:
        return Long.valueOf(partition);
      case FLOAT:
        return Float.valueOf(partition);
      case DOUBLE:
        return Double.valueOf(partition);
      case DATE:
        return LocalDate.parse(partition);
      case TIMESTAMP:
        return LocalDateTime.parse(partition);
      case DECIMAL:
        return new BigDecimal(partition);
      default:
        throw new RuntimeException(
            String.format("Can not convert %s to type %s for partition value", partition, type));
    }
  }

  public static Type.TypeID mapHudiType(DataType dataType) {
    switch (dataType) {
      case BIGINT:
        return Type.TypeID.LONG;
      case INTEGER:
        return Type.TypeID.INT;
      case FLOAT8:
        return Type.TypeID.DOUBLE;
      case REAL:
        return Type.TypeID.FLOAT;
      case NUMERIC:
        return Type.TypeID.DECIMAL;
      case TEXT:
        return Type.TypeID.STRING;
      case BOOLEAN:
        return Type.TypeID.BOOLEAN;
      case UUID:
        return Type.TypeID.UUID;
      case BYTEA:
        return Type.TypeID.BINARY;
      case TIME:
        return Type.TypeID.TIME;
      case DATE:
        return Type.TypeID.DATE;
      case TIMESTAMP_WITH_TIME_ZONE:
      case TIMESTAMP:
        return Type.TypeID.TIMESTAMP;
      default:
        throw new UnsupportedTypeException(String.format("DataType %s unsupported", dataType));
    }
  }

  /**
   * Projects the row fields with given names.
   */
  public static RowField[] projectRowFields(List<ColumnDescriptor> columns, String[] names) {
    List<RowField> result = new ArrayList<>();

    for (String name : names) {
      for (ColumnDescriptor column : columns) {
        if (name.equals(column.columnName())) {
          result.add(new RowField(name, mapHudiType(column.getDataType())));
          break;
        }
      }
    }

    return result.toArray(new RowField[0]);
  }
}
