/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.jdbc;

import java.io.InputStream;
import java.io.Reader;

import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.math.BigDecimal;
import java.net.URL;
import java.util.Calendar;
import java.util.Map;

/**
 * The <code>AbstractResultSet</code> serves as a base class for implementations
 * of the{@link java.sql.ResultSet} interface. All calls using columnNames are
 * translated into the corresponding call with index position computed using
 * a call to {@link java.sql.ResultSet#findColumn(String) findColumn}.
 *
 * @author Thomas Hallgren
 */
public abstract class AbstractResultSet implements ResultSet
{
	public String getCursorName()
	throws SQLException
	{
		return null;
	}

    public Statement getStatement()
    throws SQLException
    {
        return null;
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as an Array object in the Java programming language.
     */
    public Array getArray(String columnLabel) 
    throws SQLException
    {
        return this.getArray(this.findColumn(columnLabel));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as a stream of ASCII characters.
     */
    public InputStream getAsciiStream(String columnLabel) 
    throws SQLException
    {
        return this.getAsciiStream(this.findColumn(columnLabel));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as a java.math.BigDecimal with full precision.
     */
    public BigDecimal getBigDecimal(String columnLabel) 
    throws SQLException
    {
        return this.getBigDecimal(this.findColumn(columnLabel));
    }

    /**
     * @deprecated
     */
    public BigDecimal getBigDecimal(String columnLabel, int scale) 
    throws SQLException
    {
        return this.getBigDecimal(this.findColumn(columnLabel), scale);
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as a stream of uninterpreted bytes.
     */
    public InputStream getBinaryStream(String columnLabel) 
    throws SQLException
    {
        return this.getBinaryStream(this.findColumn(columnLabel));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as a Blob object in the Java programming language.
     */
    public Blob getBlob(String columnLabel) 
    throws SQLException
    {
        return this.getBlob(this.findColumn(columnLabel));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as a boolean in the Java programming language.
     */
    public boolean getBoolean(String columnLabel) 
    throws SQLException
    {
        return this.getBoolean(this.findColumn(columnLabel));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as a byte in the Java programming language.
     */
    public byte getByte(String columnLabel) 
    throws SQLException
    {
        return this.getByte(this.findColumn(columnLabel));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as a byte array in the Java programming language.
     */
    public byte[] getBytes(String columnLabel) 
    throws SQLException
    {
        return this.getBytes(this.findColumn(columnLabel));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as a java.io.Reader object.
     */
    public Reader getCharacterStream(String columnLabel) 
    throws SQLException
    {
        return this.getCharacterStream(this.findColumn(columnLabel));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as a Clob object in the Java programming language.
     */
    public Clob getClob(String columnLabel) 
    throws SQLException
    {
        return this.getClob(this.findColumn(columnLabel));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as a java.sql.Date object in the Java programming
     * language.
     */
    public Date getDate(String columnLabel) 
    throws SQLException
    {
        return this.getDate(this.findColumn(columnLabel));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as a java.sql.Date object in the Java programming
     * language.
     */
    public Date getDate(String columnLabel, Calendar cal) 
    throws SQLException
    {
        return this.getDate(this.findColumn(columnLabel), cal);
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as a double in the Java programming language.
     */
    public double getDouble(String columnLabel) 
    throws SQLException
    {
        return this.getDouble(this.findColumn(columnLabel));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as a float in the Java programming language.
     */
    public float getFloat(String columnLabel) 
    throws SQLException
    {
        return this.getFloat(this.findColumn(columnLabel));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as an int in the Java programming language.
     */
    public int getInt(String columnLabel) 
    throws SQLException
    {
        return this.getInt(this.findColumn(columnLabel));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as a long in the Java programming language.
     */
    public long getLong(String columnLabel) 
    throws SQLException
    {
        return this.getLong(this.findColumn(columnLabel));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as a java.io.Reader object.
     */
    public Reader getNCharacterStream(String columnLabel) 
    throws SQLException
    {
        return this.getNCharacterStream(this.findColumn(columnLabel));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as a NClob object in the Java programming language.
     */
    public java.sql.NClob getNClob(String columnLabel) 
    throws SQLException
    {
        return this.getNClob(this.findColumn(columnLabel));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as a String in the Java programming language.
     */
    public String getNString(String columnLabel) 
    throws SQLException
    {
        return this.getNString(this.findColumn(columnLabel));
    }

    /**
     * Gets the value of the designated column in the current row of this
     * ResultSet object as an Object in the Java programming language.
     */
    public Object getObject(String columnLabel) 
    throws SQLException
    {
        return this.getObject(this.findColumn(columnLabel));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as an Object in the Java programming language.
     */
    public Object getObject(String columnLabel, Map map) 
    throws SQLException
    {
        return this.getObject(this.findColumn(columnLabel), map);
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as a Ref object in the Java programming language.
     */
    public Ref getRef(String columnLabel) 
    throws SQLException
    {
        return this.getRef(this.findColumn(columnLabel));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as a java.sql.RowId object in the Java programming
     * language.
     */
    public java.sql.RowId getRowId(String columnLabel) 
    throws SQLException
    {
        return this.getRowId(this.findColumn(columnLabel));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as a short in the Java programming language.
     */
    public short getShort(String columnLabel) 
    throws SQLException
    {
        return this.getShort(this.findColumn(columnLabel));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet as a java.sql.SQLXML object in the Java programming language.
     */
    public java.sql.SQLXML getSQLXML(String columnLabel) 
    throws SQLException
    {
        return this.getSQLXML(this.findColumn(columnLabel));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as a String in the Java programming language.
     */
    public String getString(String columnLabel) 
    throws SQLException
    {
        return this.getString(this.findColumn(columnLabel));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as a java.sql.Time object in the Java programming
     * language.
     */
    public Time getTime(String columnLabel) 
    throws SQLException
    {
        return this.getTime(this.findColumn(columnLabel));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as a java.sql.Time object in the Java programming
     * language.
     */
    public Time getTime(String columnLabel, Calendar cal) 
    throws SQLException
    {
        return this.getTime(this.findColumn(columnLabel), cal);
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as a java.sql.Timestamp object in the Java programming
     * language.
     */
    public Timestamp getTimestamp(String columnLabel) 
    throws SQLException
    {
        return this.getTimestamp(this.findColumn(columnLabel));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as a java.sql.Timestamp object in the Java programming
     * language.
     */
    public Timestamp getTimestamp(String columnLabel, Calendar cal) 
    throws SQLException
    {
        return this.getTimestamp(this.findColumn(columnLabel), cal);
    }

    /**
     * @deprecated use getCharacterStream instead
     */
    public InputStream getUnicodeStream(String columnLabel) 
    throws SQLException
    {
        return this.getUnicodeStream(this.findColumn(columnLabel));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * ResultSet object as a java.net.URL object in the Java programming
     * language.
     */
    public URL getURL(String columnLabel) 
    throws SQLException
    {
        return this.getURL(this.findColumn(columnLabel));
    }

    /**
     * Updates the designated column with a java.sql.Array value.
     */
    public void updateArray(String columnLabel, Array x) 
    throws SQLException
    {
        this.updateArray(this.findColumn(columnLabel), x);
    }

    /**
     * Updates the designated column with an ascii stream value.
     */
    public void updateAsciiStream(String columnLabel, InputStream x) 
    throws SQLException
    {
        this.updateAsciiStream(this.findColumn(columnLabel), x);
    }

    /**
     * Updates the designated column with an ascii stream value, which will have
     * the specified number of bytes.
     */
    public void updateAsciiStream(String columnLabel, InputStream x, int length) 
    throws SQLException
    {
        this.updateAsciiStream(this.findColumn(columnLabel), x, length);
    }

    /**
     * Updates the designated column with an ascii stream value, which will have
     * the specified number of bytes.
     */
    public void updateAsciiStream(String columnLabel, InputStream x, long length) 
    throws SQLException
    {
        this.updateAsciiStream(this.findColumn(columnLabel), x, length);
    }

    /**
     * Updates the designated column with a java.sql.BigDecimal value.
     */
    public void updateBigDecimal(String columnLabel, BigDecimal x) 
    throws SQLException
    {
        this.updateBigDecimal(this.findColumn(columnLabel), x);
    }

    /**
     * Updates the designated column with a binary stream value.
     */
    public void updateBinaryStream(String columnLabel, InputStream x) 
    throws SQLException
    {
        this.updateBinaryStream(this.findColumn(columnLabel), x);
    }

    /**
     * Updates the designated column with a binary stream value, which will have
     * the specified number of bytes.
     */
    public void updateBinaryStream(String columnLabel, InputStream x, int length) 
    throws SQLException
    {
        this.updateBinaryStream(this.findColumn(columnLabel), x, length);
    }

    /**
     * Updates the designated column with a binary stream value, which will have
     * the specified number of bytes.
     */
    public void updateBinaryStream(String columnLabel, InputStream x, long length) 
    throws SQLException
    {
        this.updateBinaryStream(this.findColumn(columnLabel), x, length);
    }

    /**
     * Updates the designated column with a java.sql.Blob value.
     */
    public void updateBlob(String columnLabel, Blob x) 
    throws SQLException
    {
        this.updateBlob(this.findColumn(columnLabel), x);
    }

    /**
     * Updates the designated column using the given input stream.
     */
    public void updateBlob(String columnLabel, InputStream inputStream) 
    throws SQLException
    {
        this.updateBlob(this.findColumn(columnLabel), inputStream);
    }

    /**
     * Updates the designated column using the given input stream, which will
     * have the specified number of bytes.
     */
    public void updateBlob(String columnLabel, InputStream inputStream, long length) 
    throws SQLException
    {
        this.updateBlob(this.findColumn(columnLabel), inputStream, length);
    }

    /**
     * Updates the designated column with a boolean value.
     */
    public void updateBoolean(String columnLabel, boolean x) 
    throws SQLException
    {
        this.updateBoolean(this.findColumn(columnLabel), x);
    }

    /**
     * Updates the designated column with a byte value.
     */
    public void updateByte(String columnLabel, byte x) 
    throws SQLException
    {
        this.updateByte(this.findColumn(columnLabel), x);
    }

    /**
     * Updates the designated column with a byte array value.
     */
    public void updateBytes(String columnLabel, byte[] x) 
    throws SQLException
    {
        this.updateBytes(this.findColumn(columnLabel), x);
    }

    /**
     * Updates the designated column with a character stream value.
     */
    public void updateCharacterStream(String columnLabel, Reader reader) 
    throws SQLException
    {
        this.updateCharacterStream(this.findColumn(columnLabel), reader);
    }

    /**
     * Updates the designated column with a character stream value, which will
     * have the specified number of bytes.
     */
    public void updateCharacterStream(String columnLabel, Reader reader, int length) 
    throws SQLException
    {
        this.updateCharacterStream(this.findColumn(columnLabel), reader, length);
    }

    /**
     * Updates the designated column with a character stream value, which will
     * have the specified number of bytes.
     */
    public void updateCharacterStream(String columnLabel, Reader reader, long length) 
    throws SQLException
    {
        this.updateCharacterStream(this.findColumn(columnLabel), reader, length);
    }

    /**
     * Updates the designated column with a java.sql.Clob value.
     */
    public void updateClob(String columnLabel, Clob x) 
    throws SQLException
    {
        this.updateClob(this.findColumn(columnLabel), x);
    }

    /**
     * Updates the designated column using the given Reader object.
     */
    public void updateClob(String columnLabel, Reader reader) 
    throws SQLException
    {
        this.updateClob(this.findColumn(columnLabel), reader);
    }

    /**
     * Updates the designated column using the given Reader object, which is the
     * given number of characters long.
     */
    public void updateClob(String columnLabel, Reader reader, long length) 
    throws SQLException
    {
        this.updateClob(this.findColumn(columnLabel), reader, length);
    }

    /**
     * Updates the designated column with a java.sql.Date value.
     */
    public void updateDate(String columnLabel, Date x) 
    throws SQLException
    {
        this.updateDate(this.findColumn(columnLabel), x);
    }

    /**
     * Updates the designated column with a double value.
     */
    public void updateDouble(String columnLabel, double x) 
    throws SQLException
    {
        this.updateDouble(this.findColumn(columnLabel), x);
    }

    /**
     * Updates the designated column with a float value.
     */
    public void updateFloat(String columnLabel, float x) 
    throws SQLException
    {
        this.updateFloat(this.findColumn(columnLabel), x);
    }

    /**
     * Updates the designated column with an int value.
     */
    public void updateInt(String columnLabel, int x) 
    throws SQLException
    {
        this.updateInt(this.findColumn(columnLabel), x);
    }

    /**
     * Updates the designated column with a long value.
     */
    public void updateLong(String columnLabel, long x) 
    throws SQLException
    {
        this.updateLong(this.findColumn(columnLabel), x);
    }

    /**
     * Updates the designated column with a character stream value.
     */
    public void updateNCharacterStream(String columnLabel, Reader reader) 
    throws SQLException
    {
        this.updateNCharacterStream(this.findColumn(columnLabel), reader);
    }

    /**
     * Updates the designated column with a character stream value, which will
     * have the specified number of bytes.
     */
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) 
    throws SQLException
    {
        this.updateNCharacterStream(this.findColumn(columnLabel), reader, length);
    }

    /**
     * Updates the designated column with a java.sql.NClob value.
     */
    public void updateNClob(String columnLabel, java.sql.NClob nClob) 
    throws SQLException
    {
        this.updateNClob(this.findColumn(columnLabel), nClob);
    }

    /**
     * Updates the designated column using the given Reader object.
     */
    public void updateNClob(String columnLabel, Reader reader) 
    throws SQLException
    {
        this.updateNClob(this.findColumn(columnLabel), reader);
    }

    /**
     * Updates the designated column using the given Reader object, which is the
     * given number of characters long.
     */
    public void updateNClob(String columnLabel, Reader reader, long length) 
    throws SQLException
    {
        this.updateNClob(this.findColumn(columnLabel), reader, length);
    }

    /**
     * Updates the designated column with a String value.
     */
    public void updateNString(String columnLabel, String nString) 
    throws SQLException
    {
        this.updateNString(this.findColumn(columnLabel), nString);
    }

    /**
     * Updates the designated column with a null value.
     */
    public void updateNull(String columnLabel) 
    throws SQLException
    {
        this.updateNull(this.findColumn(columnLabel));
    }

    /**
     * Updates the designated column with an Object value.
     */
    public void updateObject(String columnLabel, Object x) 
    throws SQLException
    {
        this.updateObject(this.findColumn(columnLabel), x);
    }

    /**
     * Updates the designated column with an Object value.
     */
    public void updateObject(String columnLabel, Object x, int scaleOrLength) 
    throws SQLException
    {
        this.updateObject(this.findColumn(columnLabel), x, scaleOrLength);
    }

    /**
     * Updates the designated column with a java.sql.Ref value.
     */
    public void updateRef(String columnLabel, Ref x) 
    throws SQLException
    {
        this.updateRef(this.findColumn(columnLabel), x);
    }

    /**
     * Updates the designated column with a RowId value.
     */
    public void updateRowId(String columnLabel, java.sql.RowId x) 
    throws SQLException
    {
        this.updateRowId(this.findColumn(columnLabel), x);
    }

    /**
     * Updates the designated column with a short value.
     */
    public void updateShort(String columnLabel, short x) 
    throws SQLException
    {
        this.updateShort(this.findColumn(columnLabel), x);
    }

    /**
     * Updates the designated column with a java.sql.SQLXML value.
     */
    public void updateSQLXML(String columnLabel, java.sql.SQLXML xmlObject) 
    throws SQLException
    {
        this.updateSQLXML(this.findColumn(columnLabel), xmlObject);
    }

    /**
     * Updates the designated column with a String value.
     */
    public void updateString(String columnLabel, String x) 
    throws SQLException
    {
        this.updateString(this.findColumn(columnLabel), x);
    }

    /**
     * Updates the designated column with a java.sql.Time value.
     */
    public void updateTime(String columnLabel, Time x) 
    throws SQLException
    {
        this.updateTime(this.findColumn(columnLabel), x);
    }

    /**
     * Updates the designated column with a java.sql.Timestamp value.
     */
    public void updateTimestamp(String columnLabel, Timestamp x) 
    throws SQLException
    {
        this.updateTimestamp(this.findColumn(columnLabel), x);
    }

	/**
	 * Returns true if this either implements the interface argument or is
	 * directly or indirectly a wrapper for an object that does. Returns false
	 * otherwise. If this implements the interface then return true, else if
	 * this is a wrapper then return the result of recursively calling
	 * isWrapperFor on the wrapped object. If this does not implement the
	 * interface and is not a wrapper, return false. This method should be
	 * implemented as a low-cost operation compared to unwrap so that callers
	 * can use this method to avoid expensive unwrap calls that may fail. If
	 * this method returns true then calling unwrap with the same argument
	 * should succeed.
	 *
	 * @since 1.6
	 */
	public boolean isWrapperFor(Class iface)
	throws SQLException
	{
		throw new UnsupportedFeatureException("isWrapperFor");
	}

	/**
	 * Returns an object that implements the given interface to allow access to
	 * non-standard methods, or standard methods not exposed by the proxy. If
	 * the receiver implements the interface then the result is the receiver or
	 * a proxy for the receiver. If the receiver is a wrapper and the wrapped
	 * object implements the interface then the result is the wrapped object or
	 * a proxy for the wrapped object. Otherwise return the the result of
	 * calling unwrap recursively on the wrapped object or a proxy for that
	 * result. If the receiver is not a wrapper and does not implement the
	 * interface, then an SQLException is thrown.
	 *
	 * @since 1.6
	 */
	public Object unwrap(Class iface)
	throws SQLException
	{
		throw new UnsupportedFeatureException("unwrap");
	}
}