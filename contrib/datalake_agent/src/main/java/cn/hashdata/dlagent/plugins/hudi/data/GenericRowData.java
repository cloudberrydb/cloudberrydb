/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.hashdata.dlagent.plugins.hudi.data;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Objects;

public final class GenericRowData implements RowData {

    /** The array to store the actual internal format values. */
    private final Object[] fields;

    public GenericRowData(int arity) {
        this.fields = new Object[arity];
    }

    /**
     * Sets the field value at the given position.
     *
     * <p>Note: The given field value must be an internal data structures. Otherwise the {@link
     * GenericRowData} is corrupted and may throw exception when processing. See {@link RowData} for
     * more information about internal data structures.
     *
     * <p>The field value can be null for representing nullability.
     */
    public void setField(int pos, Object value) {
        this.fields[pos] = value;
    }

    /**
     * Returns the field value at the given position.
     *
     * <p>Note: The returned value is in internal data structure. See {@link RowData} for more
     * information about internal data structures.
     *
     * <p>The returned field value can be null for representing nullability.
     */
    public Object getField(int pos) {
        return this.fields[pos];
    }

    @Override
    public int getArity() {
        return fields.length;
    }

    @Override
    public boolean isNullAt(int pos) {
        return this.fields[pos] == null;
    }

    @Override
    public boolean getBoolean(int pos) {
        return (boolean) this.fields[pos];
    }

    @Override
    public byte getByte(int pos) {
        return (byte) this.fields[pos];
    }

    @Override
    public short getShort(int pos) {
        return (short) this.fields[pos];
    }

    @Override
    public int getInt(int pos) {
        return (int) this.fields[pos];
    }

    @Override
    public long getLong(int pos) {
        return (long) this.fields[pos];
    }

    @Override
    public float getFloat(int pos) {
        return (float) this.fields[pos];
    }

    @Override
    public double getDouble(int pos) {
        return (double) this.fields[pos];
    }

    @Override
    public String getString(int pos) {
        return (String) this.fields[pos];
    }

    @Override
    public Timestamp getTimestamp(int pos) {
        return (Timestamp) this.fields[pos];
    }

    @Override
    public BigDecimal getDicimal(int pos) {
        return (BigDecimal) this.fields[pos];
    }

    @Override
    public byte[] getBinary(int pos) {
        return (byte[]) this.fields[pos];
    }

    @Override
    public RowData getRow(int pos, int numFields) {
        return (RowData) this.fields[pos];
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof GenericRowData)) {
            return false;
        }
        GenericRowData that = (GenericRowData) o;
        return Arrays.deepEquals(fields, that.fields);
    }

    @Override
    public int hashCode() {
        int result = 1;
        result = 31 * result + Arrays.deepHashCode(fields);
        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        for (int i = 0; i < fields.length; i++) {
            if (i != 0) {
                sb.append(",");
            }
            sb.append(fields[i].toString());
        }
        sb.append(")");
        return sb.toString();
    }

    public static GenericRowData of(Object... values) {
        GenericRowData row = new GenericRowData(values.length);

        for (int i = 0; i < values.length; ++i) {
            row.setField(i, values[i]);
        }

        return row;
    }
}
