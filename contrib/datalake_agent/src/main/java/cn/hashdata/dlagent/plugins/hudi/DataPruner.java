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

package cn.hashdata.dlagent.plugins.hudi;

import cn.hashdata.dlagent.plugins.hudi.data.RowData;
import cn.hashdata.dlagent.plugins.hudi.data.RowField;
import org.apache.hudi.internal.schema.Type;

import javax.annotation.Nullable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility to do data skipping.
 */
public class DataPruner {
  private final String[] referencedCols;
  private final ExpressionEvaluators.Evaluator evaluator;

  private DataPruner(String[] referencedCols, ExpressionEvaluators.Evaluator evaluator) {
    this.referencedCols = referencedCols;
    this.evaluator = evaluator;
  }

  /**
   * Filters the index row with specific data filters and query fields.
   *
   * @param indexRow    The index row
   * @param queryFields The query fields referenced by the filters
   * @return true if the index row should be considered as a candidate
   */
  public boolean test(RowData indexRow, RowField[] queryFields) {
    Map<String, ColumnStats> columnStatsMap = convertColumnStats(indexRow, queryFields);
    return evaluator.eval(columnStatsMap);
  }

  public String[] getReferencedCols() {
    return referencedCols;
  }

  @Nullable
  public static DataPruner newInstance(ExpressionEvaluators.Evaluator filter, String[] referencedCols) {
    if (filter == null) {
      return null;
    }

    return new DataPruner(referencedCols, filter);
  }

  public static Map<String, ColumnStats> convertColumnStats(RowData indexRow, RowField[] queryFields) {
    if (indexRow == null || queryFields == null) {
      throw new IllegalArgumentException("Index Row and query fields could not be null.");
    }

    Map<String, ColumnStats> mapping = new LinkedHashMap<>();
    for (int i = 0; i < queryFields.length; i++) {
      String name = queryFields[i].getName();
      int startPos = 2 + i * 3;
      Type.TypeID colType = queryFields[i].getDataType();
      Object minVal = indexRow.isNullAt(startPos) ? null : getValAsObject(indexRow, startPos, colType);
      Object maxVal = indexRow.isNullAt(startPos + 1) ? null : getValAsObject(indexRow, startPos + 1, colType);
      long nullCnt = indexRow.getLong(startPos + 2);
      mapping.put(name, new ColumnStats(minVal, maxVal, nullCnt));
    }

    return mapping;
  }

  /**
   * Returns the value as Java object at position {@code pos} of row {@code indexRow}.
   */
  private static Object getValAsObject(RowData indexRow, int pos, Type.TypeID colType) {
    switch (colType) {
      // NOTE: Since we can't rely on Avro's "date", and "timestamp-micros" logical-types, we're
      //       manually encoding corresponding values as int and long w/in the Column Stats Index and
      //       here we have to decode those back into corresponding logical representation.
      case LONG:
      case TIME:
        return indexRow.getLong(pos);
      case DATE:
      case INT:
        return indexRow.getInt(pos);
      case DOUBLE:
        return indexRow.getDouble(pos);
      case FLOAT:
        return indexRow.getFloat(pos);
      case DECIMAL:
        return indexRow.getDicimal(pos);
      case STRING:
        return indexRow.getString(pos);
      case BOOLEAN:
        return indexRow.getBoolean(pos);
      case UUID:
      case BINARY:
        return indexRow.getBinary(pos);
      case TIMESTAMP:
        return indexRow.getTimestamp(pos);
      default:
        throw new UnsupportedOperationException("Unsupported type: " + colType);
    }
  }
}