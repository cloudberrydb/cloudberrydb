/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.jdbc;

import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.math.BigDecimal;
import java.net.URL;
import java.util.Calendar;
import java.util.Map;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;


/**
 * @author Thomas Hallgren
 */
public abstract class ObjectResultSet extends AbstractResultSet
{
	private boolean m_wasNull = false;


	/**
	 * This is a noop since warnings are not supported.
	 */
	public void clearWarnings()
	throws SQLException
	{
	}

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as an Array object in the Java programming language.
     */
    public Array getArray(int columnIndex) 
    throws SQLException
    {
		return (Array)this.getValue(columnIndex, Array.class);
	}

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as a stream of ASCII characters.
     */
	public InputStream getAsciiStream(int columnIndex)
	throws SQLException
	{
		Clob c = this.getClob(columnIndex);
		return (c == null) ? null : c.getAsciiStream();
	}

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as a java.math.BigDecimal with full precision.
     */
	public BigDecimal getBigDecimal(int columnIndex)
	throws SQLException
	{
		return (BigDecimal)this.getValue(columnIndex, BigDecimal.class);
	}

	/**
	 * @deprecated
	 */
	public BigDecimal getBigDecimal(int columnIndex, int scale)
	throws SQLException
	{
		throw new UnsupportedFeatureException("getBigDecimal(int, int)");
	}

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as a stream of uninterpreted bytes.
     */
	public InputStream getBinaryStream(int columnIndex)
	throws SQLException
	{
		Blob b = this.getBlob(columnIndex);
		return (b == null) ? null : b.getBinaryStream();
	}

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as a Blob object in the Java programming language.
     */
	public Blob getBlob(int columnIndex)
	throws SQLException
	{
		byte[] bytes = this.getBytes(columnIndex);
		return (bytes == null) ? null :  new BlobValue(bytes);
	}

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as a boolean in the Java programming language.
     */
	public boolean getBoolean(int columnIndex)
	throws SQLException
	{
		Boolean b = (Boolean)this.getValue(columnIndex, Boolean.class);
		return (b == null) ? false : b.booleanValue();
	}

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as a byte in the Java programming language.
     */
	public byte getByte(int columnIndex)
	throws SQLException
	{
		Number b = this.getNumber(columnIndex, byte.class);
		return (b == null) ? 0 : b.byteValue();
	}

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as a byte array in the Java programming language.
     */
	public byte[] getBytes(int columnIndex)
	throws SQLException
	{
		return (byte[])this.getValue(columnIndex, byte[].class);
	}

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as a java.io.Reader object.
     */
	public Reader getCharacterStream(int columnIndex)
	throws SQLException
	{
		Clob c = this.getClob(columnIndex);
		return (c == null) ? null : c.getCharacterStream();
	}

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as a Clob object in the Java programming language.
     */
	public Clob getClob(int columnIndex)
	throws SQLException
	{
		String str = this.getString(columnIndex);
		return (str == null) ? null :  new ClobValue(str);
	}

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as a java.sql.Date object in the Java programming
     * language.
     */	
	public Date getDate(int columnIndex)
	throws SQLException
	{
		return (Date)this.getValue(columnIndex, Date.class);
	}

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as a java.sql.Date object in the Java programming
     * language.
     */
	public Date getDate(int columnIndex, Calendar cal)
	throws SQLException
	{
		return (Date)this.getValue(columnIndex, Date.class, cal);
	}

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as a double in the Java programming language.
     */
	public double getDouble(int columnIndex)
	throws SQLException
	{
		Number d = this.getNumber(columnIndex, double.class);
		return (d == null) ? 0 : d.doubleValue();
	}

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as a float in the Java programming language.
     */
	public float getFloat(int columnIndex)
	throws SQLException
	{
		Number f = this.getNumber(columnIndex, float.class);
		return (f == null) ? 0 : f.floatValue();
	}

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as an int in the Java programming language.
     */
	public int getInt(int columnIndex)
	throws SQLException
	{
		Number i = this.getNumber(columnIndex, int.class);
		return (i == null) ? 0 : i.intValue();
	}

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as a long in the Java programming language.
     */
	public long getLong(int columnIndex)
	throws SQLException
	{
		Number l = this.getNumber(columnIndex, long.class);
		return (l == null) ? 0 : l.longValue();
	}

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as a java.io.Reader object.
     */
    public Reader getNCharacterStream(int columnIndex) 
    throws SQLException
    {
		throw new UnsupportedFeatureException("NCharacterStream is not yet implemented");		
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as a NClob object in the Java programming language.
     */
    public NClob getNClob(int columnIndex) 
    throws SQLException
    {
		throw new UnsupportedFeatureException("NClob is not yet implemented");
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as a String in the Java programming language.
     */
    public String getNString(int columnIndex) 
    throws SQLException
    {
		throw new UnsupportedFeatureException("NString is not yet implemented");
    }

	/**
	 * ResultSetMetaData is not yet supported.
	 * @throws SQLException indicating that this feature is not supported.
	 */
	public ResultSetMetaData getMetaData()
	throws SQLException
	{
		throw new UnsupportedFeatureException("ResultSet meta data is not yet implemented");
	}

    /**
     * Gets the value of the designated column in the current row of this
     * ResultSet object as an Object in the Java programming language.
     */
	public final Object getObject(int columnIndex)
	throws SQLException
	{
		Object value = this.getObjectValue(columnIndex);
		m_wasNull = (value == null);
		return value;
	}

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as an Object in the Java programming language.
     */
	public final Object getObject(int columnIndex, Map map)
	throws SQLException
	{
		Object value = this.getObjectValue(columnIndex, map);
		m_wasNull = (value == null);
		return value;
	}

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as a Ref object in the Java programming language.
     */
	public Ref getRef(int columnIndex)
	throws SQLException
	{
		return (Ref)this.getValue(columnIndex, Ref.class);
	}

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as a java.sql.RowId object in the Java programming
     * language.
     */
    public RowId getRowId(int columnIndex) 
    throws SQLException
    {
		throw new UnsupportedFeatureException("RowId is not yet implemented");
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as a short in the Java programming language.
     */
	public short getShort(int columnIndex)
	throws SQLException
	{
		Number s = this.getNumber(columnIndex, short.class);
		return (s == null) ? 0 : s.shortValue();
	}

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet as a java.sql.SQLXML object in the Java programming language.
     */
    public SQLXML getSQLXML(int columnIndex) 
    throws SQLException
    {
		throw new UnsupportedFeatureException("SQLXML is not yet implemented");
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as a String in the Java programming language.
     */
	public String getString(int columnIndex)
	throws SQLException
	{
		return (String)this.getValue(columnIndex, String.class);
	}

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as a java.sql.Time object in the Java programming
     * language.
     */
	public Time getTime(int columnIndex)
	throws SQLException
	{
		return (Time)this.getValue(columnIndex, Time.class);
	}

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as a java.sql.Time object in the Java programming
     * language.
     */
	public Time getTime(int columnIndex, Calendar cal)
	throws SQLException
	{
		return (Time)this.getValue(columnIndex, Time.class, cal);
	}

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as a java.sql.Timestamp object in the Java programming
     * language.
     */
	public Timestamp getTimestamp(int columnIndex)
	throws SQLException
	{
		return (Timestamp)this.getValue(columnIndex, Timestamp.class);
	}

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as a java.sql.Timestamp object in the Java programming
     * language.
     */
	public Timestamp getTimestamp(int columnIndex, Calendar cal)
	throws SQLException
	{
		return (Timestamp)this.getValue(columnIndex, Timestamp.class, cal);
	}


    /**
     * @deprecated - use getCharacterStream in place of getUnicodeStream
	 */
	public InputStream getUnicodeStream(int columnIndex)
	throws SQLException
	{
		throw new UnsupportedFeatureException("ResultSet.getUnicodeStream");
	}

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as a java.net.URL object in the Java programming
     * language.
     */
	public URL getURL(int columnIndex) 
    throws SQLException
	{
		return (URL)this.getValue(columnIndex, URL.class);
	}

	/**
	 *
	 */
	public SQLWarning getWarnings()
	throws SQLException
	{
		return null;
	}

	/**
	 * Refresh row is not yet implemented.
	 * @throws SQLException indicating that this feature is not supported.
	 */
	public void refreshRow()
	throws SQLException
	{
		throw new UnsupportedFeatureException("Refresh row");
	}

    /**
     * Updates the designated column with a java.sql.Array value.
     */
	public void updateArray(int columnIndex, Array x) 
	throws SQLException
	{
		this.updateObject(columnIndex, x);
	}

    /**
     * Updates the designated column with an ascii stream value.
     */
    public void updateAsciiStream(int columnIndex, InputStream x) 
    throws SQLException
    {
		try {
			this.updateObject(columnIndex, 
				    new ClobValue(new InputStreamReader(x, "US-ASCII")));
		}
		catch(UnsupportedEncodingException e)
		{
			throw new SQLException("US-ASCII encoding is not supported by this JVM");
		}												 
    }

    /**
     * Updates the designated column with an ascii stream value, which will have
     * the specified number of bytes.
     */
	public void updateAsciiStream(int columnIndex, InputStream x, int length)
	throws SQLException
	{
		try
		{
			this.updateObject(columnIndex,
					new ClobValue(new InputStreamReader(x, "US-ASCII"), length));
		}
		catch(UnsupportedEncodingException e)
		{
			throw new SQLException("US-ASCII encoding is not supported by this JVM");
		}
	}

    /**
     * Updates the designated column with an ascii stream value, which will have
     * the specified number of bytes.
     */
	public void updateAsciiStream(int columnIndex, InputStream x, long length)
	throws SQLException
	{
		try
		{
			this.updateObject(columnIndex,
					new ClobValue(new InputStreamReader(x, "US-ASCII"), length));
		}
		catch(UnsupportedEncodingException e)
		{
			throw new SQLException("US-ASCII encoding is not supported by this JVM");
		}
	}

    /**
     * Updates the designated column with a java.math.BigDecimal value.
     */
	public void updateBigDecimal(int columnIndex, BigDecimal x)
	throws SQLException
	{
		this.updateObject(columnIndex, x);
	}

    /**
     * Updates the designated column with a binary stream value.
     */
	public void updateBinaryStream(int columnIndex, InputStream x)
	throws SQLException
	{
		this.updateObject(columnIndex, new BlobValue(x));
	}

    /**
     * Updates the designated column with a binary stream value, which will have
     * the specified number of bytes.
     */
	public void updateBinaryStream(int columnIndex, InputStream x, int length)
	throws SQLException
	{
		this.updateObject(columnIndex, new BlobValue(x, length));
	}

    /**
     * Updates the designated column with a binary stream value, which will have
     * the specified number of bytes.
     */
	public void updateBinaryStream(int columnIndex, InputStream x, long length)
	throws SQLException
	{
		this.updateObject(columnIndex, new BlobValue(x, length));
	}

    /**
     * Updates the designated column with a java.sql.Blob value.
     */
	public void updateBlob(int columnIndex, Blob x)
	throws SQLException
	{
		this.updateObject(columnIndex, x);
	}

    /**
     * Updates the designated column using the given input stream.
     */
    public void updateBlob(int columnIndex, InputStream x) 
    throws SQLException
    {
		this.updateObject(columnIndex, new BlobValue(x));
	}

    /**
     * Updates the designated column using the given input stream, which will
     * have the specified number of bytes.
     */
    public void updateBlob(int columnIndex, InputStream x, long length) 
    throws SQLException
    {
		this.updateObject(columnIndex, new BlobValue(x, length));
	}

    /**
     * Updates the designated column with a boolean value.
     */
	public void updateBoolean(int columnIndex, boolean x)
	throws SQLException
	{
		this.updateObject(columnIndex, x ? Boolean.TRUE : Boolean.FALSE);
	}

    /**
     * Updates the designated column with a byte value.
     */
	public void updateByte(int columnIndex, byte x)
	throws SQLException
	{
		this.updateObject(columnIndex, new Byte(x));
	}

    /**
     * Updates the designated column with a byte array value.
     */
	public void updateBytes(int columnIndex, byte[] x)
	throws SQLException
	{
		this.updateObject(columnIndex, x);
	}

    /**
     * Updates the designated column with a character stream value.
     */
	public void updateCharacterStream(int columnIndex, Reader x)
	throws SQLException
	{
		this.updateObject(columnIndex, new ClobValue(x));
	}

    /**
     * Updates the designated column with a character stream value, which will
     * have the specified number of bytes.
     */
    public void updateCharacterStream(int columnIndex, Reader x, int length) 
    throws SQLException
	{
		this.updateObject(columnIndex, new ClobValue(x, length));
	}

    /**
     * Updates the designated column with a character stream value, which will
     * have the specified number of bytes.
     */
    public void updateCharacterStream(int columnIndex, Reader x, long length)
    throws SQLException
	{
		this.updateObject(columnIndex, new ClobValue(x, length));
	}

    /**
     * Updates the designated column with a java.sql.Clob value.
     */
	public void updateClob(int columnIndex, Clob x)
	throws SQLException
	{
		this.updateObject(columnIndex, x);
	}

    /**
     * Updates the designated column using the given Reader object.
     */
    public void updateClob(int columnIndex, Reader x) 
    throws SQLException
	{
		this.updateObject(columnIndex, new ClobValue(x));
	}

    /**
     * Updates the designated column using the given Reader object, which is the
     * given number of characters long.
     */
    public void updateClob(int columnIndex, Reader x, long length) 
    throws SQLException
	{
		this.updateObject(columnIndex, new ClobValue(x, length));
	}

    /**
     * Updates the designated column with a java.sql.Date value.
     */	
	public void updateDate(int columnIndex, Date x)
	throws SQLException
	{
		this.updateObject(columnIndex, x);
	}

    /**
     * Updates the designated column with a double value.
     */
	public void updateDouble(int columnIndex, double x)
	throws SQLException
	{
		this.updateObject(columnIndex, new Double(x));
	}

    /**
     * Updates the designated column with a float value.
     */
	public void updateFloat(int columnIndex, float x)
	throws SQLException
	{
		this.updateObject(columnIndex, new Float(x));
	}

    /**
     * Updates the designated column with an int value.
     */
	public void updateInt(int columnIndex, int x)
	throws SQLException
	{
		this.updateObject(columnIndex, new Integer(x));
	}

    /**
     * Updates the designated column with a long value.
     */
	public void updateLong(int columnIndex, long x)
	throws SQLException
	{
		this.updateObject(columnIndex, new Long(x));
	}

    /**
     * Updates the designated column with a character stream value.
     */
    public void updateNCharacterStream(int columnIndex, Reader x) 
    throws SQLException
    {
		throw new UnsupportedFeatureException("NCharacterStream is not yet implemented");
    }

    /**
     * Updates the designated column with a character stream value, which will
     * have the specified number of bytes.
     */
    public void updateNCharacterStream(int columnIndex, Reader x, long length) 
    throws SQLException
    {
		throw new UnsupportedFeatureException("NCharacterStream is not yet implemented");
    }

    /**
     * Updates the designated column with a java.sql.NClob value.
     */
    public void updateNClob(int columnIndex, NClob nClob) 
    throws SQLException
    {
		throw new UnsupportedFeatureException("NClob is not yet implemented");
    }

    /**
     * Updates the designated column using the given Reader The data will be read
     * from the stream as needed until end-of-stream is reached.
     */
    public void updateNClob(int columnIndex, Reader reader) 
    throws SQLException
    {
		throw new UnsupportedFeatureException("NClob is not yet implemented");
    }

    /**
     * Updates the designated column using the given Reader object, which is the
     * given number of characters long.
     */
    public void updateNClob(int columnIndex, Reader reader, long length) 
    throws SQLException
    {
		throw new UnsupportedFeatureException("NClob is not yet implemented");
    }

    /**
     * Updates the designated column with a String value.
     */
    public void updateNString(int columnIndex, String nString) 
    throws SQLException
    {
		throw new UnsupportedFeatureException("NString is not yet implemented");
    }

    /**
     * Updates the designated column with a null value.
     */
	public void updateNull(int columnIndex)
	throws SQLException
	{
		this.updateObject(columnIndex, null);
	}

    /**
     * Updates the designated column with a java.sql.Ref value.
     */
	public void updateRef(int columnIndex, Ref x)
	throws SQLException
	{
		this.updateObject(columnIndex, x);
	}

    /**
     * Updates the designated column with a RowId value.
     */
    public void updateRowId(int columnIndex, RowId x) 
    throws SQLException    
	{
		throw new UnsupportedFeatureException("RowId is not yet implemented");
	}

    /**
     * Updates the designated column with a short value.
     */
	public void updateShort(int columnIndex, short x)
	throws SQLException
	{
		this.updateObject(columnIndex, new Short(x));
	}

    /**
     * Updates the designated column with a java.sql.SQLXML value.
     */
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) 
    throws SQLException
    {
		throw new UnsupportedFeatureException("SQLXML is not yet implemented");
	}

    /**
     * Updates the designated column with a String value.
     */
	public void updateString(int columnIndex, String x)
	throws SQLException
	{
		this.updateObject(columnIndex, x);
	}

    /**
     * Updates the designated column with a java.sql.Time value.
     */
	public void updateTime(int columnIndex, Time x)
	throws SQLException
	{
		this.updateObject(columnIndex, x);
	}

    /**
     * Updates the designated column with a java.sql.Timestamp value.
     */
	public void updateTimestamp(int columnIndex, Timestamp x)
	throws SQLException
	{
		this.updateObject(columnIndex, x);
	}



	public boolean wasNull()
	{
		return m_wasNull;
	}

	protected final Number getNumber(int columnIndex, Class cls)
	throws SQLException
	{
		Object value = this.getObjectValue(columnIndex);
		m_wasNull = (value == null);
		return SPIConnection.basicNumericCoersion(cls, value);
	}

	protected final Object getValue(int columnIndex, Class cls)
	throws SQLException
	{
		return SPIConnection.basicCoersion(cls, this.getObject(columnIndex));
	}

	protected Object getValue(int columnIndex, Class cls, Calendar cal)
	throws SQLException
	{
		return SPIConnection.basicCalendricalCoersion(cls, this.getObject(columnIndex), cal);
	}

	protected Object getObjectValue(int columnIndex, Map typeMap)
	throws SQLException
	{
		if(typeMap == null)
			return this.getObjectValue(columnIndex);
		throw new UnsupportedFeatureException("Obtaining values using explicit Map");
	}

	/**
	 * Abstract getObjectValue() 
	 **/
	protected abstract Object getObjectValue(int columnIndex)
	throws SQLException;
}
