/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */

package org.postgresql.pljava.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.DriverManager;

import org.postgresql.pljava.internal.Oid;

/**
 * Implementation of ResultSetMetaData for SyntheticResultSet
 *
 * @author Filip Hrbek
 */
public abstract class AbstractResultSetMetaData implements ResultSetMetaData
{

	private Connection m_conn;

	/**
	 * Constructor.
	 */
	public AbstractResultSetMetaData()
	{
		m_conn = null;
	}

    /**
     * Returns the number of columns in this <code>ResultSet</code> object.
     *
     * @return the number of columns
     * @exception SQLException if a database access error occurs
     */
    public abstract int getColumnCount() throws SQLException;

    /**
     * Indicates whether the designated column is automatically numbered, thus read-only.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public abstract boolean isAutoIncrement(int column) throws SQLException;

	/**
     * Indicates whether a column's case matters.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public final boolean isCaseSensitive(int column) throws SQLException
	{
		checkColumnIndex(column);
		Oid oid = this.getOid(column);
		return  (  oid.equals(TypeOid.TEXT)
				|| oid.equals(TypeOid.BYTEA)
				|| oid.equals(TypeOid.VARCHAR)
				|| oid.equals(TypeOid.BPCHAR));
	}

    /**
     * Indicates whether the designated column can be used in a where clause.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public final boolean isSearchable(int column) throws SQLException
	{
		checkColumnIndex(column);
		return true;
	}

    /**
     * Indicates whether the designated column is a cash value.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public final boolean isCurrency(int column) throws SQLException
	{
		checkColumnIndex(column);
		//For now it is false; it can be changed when TypeOid.java
		//contains currency data types.
		return false;
	}
 
    /**
     * Indicates the nullability of values in the designated column.		
     *
     * @param column the first column is 1, the second is 2, ...
     * @return the nullability status of the given column; one of <code>columnNoNulls</code>,
     *          <code>columnNullable</code> or <code>columnNullableUnknown</code>
     * @exception SQLException if a database access error occurs
     */
    public final int isNullable(int column) throws SQLException
	{
		checkColumnIndex(column);
		return columnNullableUnknown;
	}

    /**
     * Indicates whether values in the designated column are signed numbers.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public final boolean isSigned(int column) throws SQLException
	{
		checkColumnIndex(column);
		Oid oid = this.getOid(column);
		return  (  oid.equals(TypeOid.INT2)
				|| oid.equals(TypeOid.INT4)
				|| oid.equals(TypeOid.INT8)
				|| oid.equals(TypeOid.FLOAT4)
				|| oid.equals(TypeOid.FLOAT8));
	}

    /**
     * Indicates the designated column's normal maximum width in characters.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return the normal maximum number of characters allowed as the width
     *          of the designated column
     * @exception SQLException if a database access error occurs
     */
    public final int getColumnDisplaySize(int column) throws SQLException
	{
		checkColumnIndex(column);
		Oid oid = this.getOid(column);

		if(oid.equals(TypeOid.INT2))
			return 6;

		if(oid.equals(TypeOid.INT4)
		|| oid.equals(TypeOid.FLOAT4))
			return 11;

		if(oid.equals(TypeOid.INT8)
		|| oid.equals(TypeOid.NUMERIC)
		|| oid.equals(TypeOid.FLOAT8)
		|| oid.equals(TypeOid.OID))
			return 20;

		if(oid.equals(TypeOid.BOOL))
			return 3;

		if(oid.equals(TypeOid.DATE))
			return 13;

		if(oid.equals(TypeOid.TIME))
			return 10;

		if(oid.equals(TypeOid.TIMESTAMP)
		|| oid.equals(TypeOid.TIMESTAMPTZ))
			return 25;

		return getFieldLength(column);
	}

    /**
     * Gets the designated column's suggested title for use in printouts and
     * displays.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return the suggested column title
     * @exception SQLException if a database access error occurs
     */
    public abstract String getColumnLabel(int column) throws SQLException;

    /**
     * Get the designated column's name.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return column name
     * @exception SQLException if a database access error occurs
     */
    public String getColumnName(int column) throws SQLException
	{
		checkColumnIndex(column);
		return getColumnLabel(column);
	}

    /**
     * Get the designated column's table's schema.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return schema name or "" if not applicable
     * @exception SQLException if a database access error occurs
     */
    public final String getSchemaName(int column) throws SQLException
	{
		checkColumnIndex(column);
		return "";
	}

    /**
     * Get the designated column's number of decimal digits.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return precision
     * @exception SQLException if a database access error occurs
     */
    public final int getPrecision(int column) throws SQLException
	{
		checkColumnIndex(column);
		Oid oid = this.getOid(column);

		if(oid.equals(TypeOid.INT2))
			return 5;

		if(oid.equals(TypeOid.INT4))
			return 10;

		if(oid.equals(TypeOid.INT8)
		|| oid.equals(TypeOid.OID))
			return 20;

		if(oid.equals(TypeOid.FLOAT4))
			return 8;

		if(oid.equals(TypeOid.FLOAT8))
			return 16;

		if(oid.equals(TypeOid.BOOL))
			return 1;

		if(oid.equals(TypeOid.NUMERIC))
			return -1;

		return 0;
	}

	/**
     * Gets the designated column's number of digits to right of the decimal point.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return scale
     * @exception SQLException if a database access error occurs
     */
    public final int getScale(int column) throws SQLException
	{
		checkColumnIndex(column);
		Oid oid = this.getOid(column);

		if(oid.equals(TypeOid.FLOAT4))
			return 8;
		
		if(oid.equals(TypeOid.FLOAT8))
			return 16;
		
		if(oid.equals(TypeOid.NUMERIC))
			return -1;

		return 0;
	}

    /**
     * Gets the designated column's table name. 
     *
     * @param column the first column is 1, the second is 2, ...
     * @return table name or "" if not applicable
     * @exception SQLException if a database access error occurs
     */
    public final String getTableName(int column) throws SQLException
	{
		checkColumnIndex(column);
		return "";
	}

    /**
     * Gets the designated column's table's catalog name.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return the name of the catalog for the table in which the given column
     *          appears or "" if not applicable
     * @exception SQLException if a database access error occurs
     */
    public final String getCatalogName(int column) throws SQLException
	{
		checkColumnIndex(column);
		return "";
	}

    /**
     * Retrieves the designated column's SQL type.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return SQL type from {@link java.sql.Types}
     * @exception SQLException if a database access error occurs
     * @see java.sql.Types
     */
    public final int getColumnType(int column) throws SQLException
	{
		checkColumnIndex(column);
		return ((SPIConnection)getDefaultConnection()).getSQLType(getOid(column));
	}

    /**
     * Retrieves the designated column's database-specific type name.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return type name used by the database. If the column type is
     * a user-defined type, then a fully-qualified type name is returned.
     * @exception SQLException if a database access error occurs
     */
    public final String getColumnTypeName(int column) throws SQLException
	{
		checkColumnIndex(column);
		return ((SPIConnection)getDefaultConnection()).getPGType(getOid(column));
	}

    /**
     * Indicates whether the designated column is definitely not writable.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public final boolean isReadOnly(int column) throws SQLException
	{
		checkColumnIndex(column);
		return true;
	}

    /**
     * Indicates whether it is possible for a write on the designated column to succeed.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public final boolean isWritable(int column) throws SQLException
	{
		checkColumnIndex(column);
		return false;
	}

    /**
     * Indicates whether a write on the designated column will definitely succeed.	
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public final boolean isDefinitelyWritable(int column) throws SQLException
	{
		checkColumnIndex(column);
		return false;
	}

    //--------------------------JDBC 2.0-----------------------------------

    /**
     * <p>Returns the fully-qualified name of the Java class whose instances 
     * are manufactured if the method <code>ResultSet.getObject</code>
     * is called to retrieve a value 
     * from the column.  <code>ResultSet.getObject</code> may return a subclass of the
     * class returned by this method.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return the fully-qualified name of the class in the Java programming
     *         language that would be used by the method 
     * <code>ResultSet.getObject</code> to retrieve the value in the specified
     * column. This is the class name used for custom mapping.
     * @exception SQLException if a database access error occurs
     * @since 1.2
     */
    public abstract String getColumnClassName(int column) throws SQLException;

    /**
     * Checks if the column index is valid.
     *
     * @param column the first column is 1, the second is 2, ...
     * @exception SQLException if the column is out of index bounds
     */
    protected abstract void checkColumnIndex(int column) throws SQLException;

	/**
	 * Gets column OID
	 * @param column Column index
	 * @return column OID
	 * @throws SQLException if an error occurs
	 */
	protected abstract Oid getOid(int column) throws SQLException;

	/**
	 * Gets column length
	 * @param column Column index
	 * @return column length
	 * @throws SQLException if an error occurs
	 */
	protected abstract int getFieldLength(int column) throws SQLException;
	/**
	 * Obtains connection to the database if needed.
	 * Once called, it stores the connection reference to a private variable.
	 */

	private Connection getDefaultConnection() throws SQLException
	{
		if (m_conn == null)
		{
			m_conn = DriverManager.getConnection("jdbc:default:connection");
		}

		return m_conn;
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
