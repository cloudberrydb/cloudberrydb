/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root directory of this distribution or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.jdbc;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Ref;
import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.SQLOutput;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;

import org.postgresql.pljava.internal.Tuple;
import org.postgresql.pljava.internal.TupleDesc;

/**
 * @author Thomas Hallgren
 */
public class SQLOutputToTuple implements SQLOutput
{
	private final Object[] m_values;
	private final TupleDesc m_tupleDesc;
	private int m_index;
	private Tuple m_tuple;

	public SQLOutputToTuple(TupleDesc tupleDesc)
	{
		m_tupleDesc = tupleDesc;
		m_values = new Object[tupleDesc.size()];
		m_index = 0;
	}

	/**
	 * Creates a tuple from the written values and returns its native pointer.
	 * All values must have been written. This method is called automatically by
	 * the trigger handler and should not be called in any other way.
	 * 
	 * @return The Tuple reflecting the current row values.
	 * @throws SQLException
	 */
	public long getTuple()
	throws SQLException
	{
		if(m_tuple != null)
			return m_tuple.getNativePointer();

		if(m_index < m_values.length)
			throw new SQLException("Too few values have been written");

		m_tuple = m_tupleDesc.formTuple(m_values);
		return m_tuple.getNativePointer();
	}

	public void writeArray(Array value) throws SQLException
	{
		this.writeValue(value);
	}

	public void writeAsciiStream(InputStream value) throws SQLException
	{
		try
		{
			Reader rdr = new BufferedReader(new InputStreamReader(value, "US-ASCII"));
			this.writeClob(new ClobValue(rdr, ClobValue.getReaderLength(rdr)));
		}
		catch(UnsupportedEncodingException e)
		{
			throw new SQLException(e.toString());
		}
	}

	public void writeBigDecimal(BigDecimal value) throws SQLException
	{
		this.writeValue(value);
	}

	public void writeBinaryStream(InputStream value) throws SQLException
	{
		if(!value.markSupported())
			value = new BufferedInputStream(value);
		this.writeBlob(new BlobValue(value, BlobValue.getStreamLength(value)));
	}

	public void writeBlob(Blob value) throws SQLException
	{
		this.writeValue(value);
	}

	public void writeBoolean(boolean value) throws SQLException
	{
		this.writeValue(value ? Boolean.TRUE : Boolean.FALSE);
	}

	public void writeByte(byte value) throws SQLException
	{
		this.writeValue(new Byte(value));
	}

	public void writeBytes(byte[] value) throws SQLException
	{
		this.writeValue(value);
	}

	public void writeCharacterStream(Reader value) throws SQLException
	{
		if(!value.markSupported())
			value = new BufferedReader(value);
		this.writeClob(new ClobValue(value, ClobValue.getReaderLength(value)));
	}

	public void writeClob(Clob value) throws SQLException
	{
		this.writeValue(value);
	}

	public void writeNClob(java.sql.NClob value) 
	throws SQLException
	{
		throw new UnsupportedOperationException("NClob not yet supported");
	}

	public void writeDate(Date value) throws SQLException
	{
		this.writeValue(value);
	}

	public void writeDouble(double value) throws SQLException
	{
		this.writeValue(new Double(value));
	}

	public void writeFloat(float value) throws SQLException
	{
		this.writeValue(new Float(value));
	}

	public void writeInt(int value) throws SQLException
	{
		this.writeValue(new Integer(value));
	}

	public void writeLong(long value) throws SQLException
	{
		this.writeValue(new Long(value));
	}

	public void writeObject(SQLData value) throws SQLException
	{
		this.writeValue(value);
	}

	public void writeRef(Ref value) throws SQLException
	{
		this.writeValue(value);
	}

	public void writeShort(short value) throws SQLException
	{
		this.writeValue(new Short(value));
	}

	public void writeString(String value) throws SQLException
	{
		this.writeValue(value);
	}

	public void writeNString(String value)
	throws SQLException
	{
		throw new UnsupportedOperationException("NString not yet supported");
	}

	public void writeStruct(Struct value) throws SQLException
	{
		this.writeValue(value);
	}

	public void writeTime(Time value) throws SQLException
	{
		this.writeValue(value);
	}

	public void writeTimestamp(Timestamp value) throws SQLException
	{
		this.writeValue(value);
	}

	public void writeURL(URL value) throws SQLException
	{
		this.writeValue(value.toString());
	}

	public void writeRowId(java.sql.RowId value) throws SQLException
	{
		throw new UnsupportedOperationException("RowId not yet supported");
	}

	public void writeSQLXML(java.sql.SQLXML value) throws SQLException
	{
		throw new UnsupportedOperationException("SQLXML not yet supported");
	}

	private void writeValue(Object value) throws SQLException
	{
		if(m_index >= m_values.length)
			throw new SQLException("Tuple cannot take more values");
		m_values[m_index++] = value;
	}
}
