/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Ref;
import java.sql.SQLException;
import java.sql.SQLInput;
import java.sql.Time;
import java.sql.Timestamp;

import org.postgresql.pljava.internal.Backend;
import org.postgresql.pljava.internal.JavaWrapper;
import org.postgresql.pljava.internal.TupleDesc;

/**
 * A single row, updateable ResultSet specially made for triggers. The
 * changes made to this ResultSet are remembered and converted to a
 * SPI_modify_tuple call prior to function return.
 *
 * @author Thomas Hallgren
 */
public class SQLInputFromTuple extends JavaWrapper implements SQLInput
{
	private int m_index;
	private final TupleDesc m_tupleDesc;
	private boolean m_wasNull;

	public SQLInputFromTuple(long heapTupleHeaderPointer, TupleDesc tupleDesc)
	throws SQLException
	{
		super(heapTupleHeaderPointer);
		m_tupleDesc = tupleDesc;
		m_index   = 0;
		m_wasNull = false;
	}

	public Array readArray() throws SQLException
	{
		return (Array)this.readValue(Array.class);
	}

	public InputStream readAsciiStream() throws SQLException
	{
		Clob c = this.readClob();
		return (c == null) ? null : c.getAsciiStream();
	}

	public BigDecimal readBigDecimal() throws SQLException
	{
		return (BigDecimal)this.readValue(BigDecimal.class);
	}

	public InputStream readBinaryStream() throws SQLException
	{
		Blob b = this.readBlob();
		return (b == null) ? null : b.getBinaryStream();
	}

	public Blob readBlob() throws SQLException
	{
		byte[] bytes = this.readBytes();
		return (bytes == null) ? null :  new BlobValue(bytes);
	}

	public boolean readBoolean() throws SQLException
	{
		Boolean b = (Boolean)this.readValue(Boolean.class);
		return (b == null) ? false : b.booleanValue();
	}

	public byte readByte() throws SQLException
	{
		Number b = this.readNumber(byte.class);
		return (b == null) ? 0 : b.byteValue();
	}

	public byte[] readBytes() throws SQLException
	{
		return (byte[])this.readValue(byte[].class);
	}

	public Reader readCharacterStream() throws SQLException
	{
		Clob c = this.readClob();
		return (c == null) ? null : c.getCharacterStream();
	}

	public Clob readClob() throws SQLException
	{
		String str = this.readString();
		return (str == null) ? null :  new ClobValue(str);
	}

	public java.sql.NClob readNClob()
	throws SQLException
	{
		throw new UnsupportedOperationException("readNClob");
	}

	public Date readDate() throws SQLException
	{
		return (Date)this.readValue(Date.class);
	}

	public double readDouble() throws SQLException
	{
		Number d = this.readNumber(double.class);
		return (d == null) ? 0 : d.doubleValue();
	}

	public float readFloat() throws SQLException
	{
		Number f = this.readNumber(float.class);
		return (f == null) ? 0 : f.floatValue();
	}

	public int readInt() throws SQLException
	{
		Number i = this.readNumber(int.class);
		return (i == null) ? 0 : i.intValue();
	}

	public long readLong() throws SQLException
	{
		Number l = this.readNumber(long.class);
		return (l == null) ? 0 : l.longValue();
	}

	public Object readObject() throws SQLException
	{
		if(m_index < m_tupleDesc.size())
		{
			Object v;
			synchronized(Backend.THREADLOCK)
			{
				v = _getObject(this.getNativePointer(), m_tupleDesc.getNativePointer(), ++m_index);
			}
			m_wasNull = v == null;
			return v;
		}
		throw new SQLException("Tuple has no more columns");
	}

	public Ref readRef() throws SQLException
	{
		return (Ref)this.readValue(Ref.class);
	}

	public short readShort() throws SQLException
	{
		Number s = this.readNumber(short.class);
		return (s == null) ? 0 : s.shortValue();
	}

	public String readString() throws SQLException
	{
		return (String)this.readValue(String.class);
	}

	public String readNString() 
    throws SQLException
	{
		throw new UnsupportedOperationException("NString not yet supported");
	}

	public Time readTime() throws SQLException
	{
		return (Time)this.readValue(Time.class);
	}

	public Timestamp readTimestamp() throws SQLException
	{
		return (Timestamp)this.readValue(Timestamp.class);
	}

	public URL readURL() throws SQLException
	{
		return (URL)this.readValue(URL.class);
	}

	public java.sql.RowId readRowId()
	throws SQLException
	{
		throw new UnsupportedOperationException("RowId not yet supported");
	}

	public java.sql.SQLXML readSQLXML()
	throws SQLException
	{
		throw new UnsupportedOperationException("SQLXML not yet supported");
	}

	public boolean wasNull() throws SQLException
	{
		return m_wasNull;
	}

	private Number readNumber(Class numberClass) throws SQLException
	{
		return SPIConnection.basicNumericCoersion(numberClass, this.readObject());
	}

	private Object readValue(Class valueClass) throws SQLException
	{
		return SPIConnection.basicCoersion(valueClass, this.readObject());
	}

	protected native void _free(long pointer);

	private static native Object _getObject(long pointer, long tupleDescPointer, int index)
	throws SQLException;
}
