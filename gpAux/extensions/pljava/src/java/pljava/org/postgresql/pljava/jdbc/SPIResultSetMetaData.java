/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */

package org.postgresql.pljava.jdbc;

import java.sql.SQLException;

import org.postgresql.pljava.internal.Oid;
import org.postgresql.pljava.internal.TupleDesc;

/**
 * Implementation of ResultSetMetaData for SPIResultSet
 *
 * @author Filip Hrbek
 */

public class SPIResultSetMetaData extends AbstractResultSetMetaData
{

	private final TupleDesc m_tupleDesc;

	/**
	 * Constructor.
	 * @param tupleDesc The descriptor for the ResultSet tuples
	 */
	public SPIResultSetMetaData(TupleDesc tupleDesc)
	{
		super();
		m_tupleDesc = tupleDesc;
	}

    /**
     * Returns the number of columns in this <code>ResultSet</code> object.
     *
     * @return the number of columns
     * @exception SQLException if a database access error occurs
     */
    public final int getColumnCount() throws SQLException
	{
		return m_tupleDesc.size();
	}

    /**
     * Indicates whether the designated column is automatically numbered, thus read-only.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public final boolean isAutoIncrement(int column) throws SQLException
	{
		checkColumnIndex(column);
		//Could SPIResultSet detect autoincrement columns???
		return false;
	}

    /**
     * Gets the designated column's suggested title for use in printouts and
     * displays.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return the suggested column title
     * @exception SQLException if a database access error occurs
     */
    public final String getColumnLabel(int column) throws SQLException
	{
		checkColumnIndex(column);
		return m_tupleDesc.getColumnName(column).toUpperCase();
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
    public final String getColumnClassName(int column) throws SQLException
	{
		checkColumnIndex(column);
		return this.getOid(column).getJavaClass().getName();
	}

    /**
     * Checks if the column index is valid.
     *
     * @param column the first column is 1, the second is 2, ...
     * @exception SQLException if the column is out of index bounds
     */
    protected final void checkColumnIndex(int column) throws SQLException
	{
		if (column < 1 || column > m_tupleDesc.size())
		{
			throw new SQLException("Invalid column index: " + column);
		}
	}

	/**
	 * Gets column OID
	 * @param column Column index
	 * @return column OID
	 * @throws SQLException if an error occurs
	 */
	protected final Oid getOid(int column) throws SQLException
	{
		return m_tupleDesc.getOid(column);
	}

	/**
	 * Gets column length. This method is called if the AbstractResultSet does not
	 * know how to get column length according to type OID.
	 * We retutn 0 because we don't know the proper length either.
	 * @param column Column index
	 * @return column length
	 * @throws SQLException if an error occurs
	 */
	protected final int getFieldLength(int column) throws SQLException
	{
		return 0;
	}
}
