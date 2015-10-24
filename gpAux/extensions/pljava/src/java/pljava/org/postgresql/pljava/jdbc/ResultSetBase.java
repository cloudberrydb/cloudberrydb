/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.jdbc;

import java.sql.SQLException;

/**
 * A ResultSet base provides methods that are common both for
 * a SyntheticResultSet (which is not associated with a
 * statement) and SPIResultSet.
 *
 * @author Filip Hrbek
 */
abstract class ResultSetBase extends ReadOnlyResultSet
{
	private int m_fetchSize;
	private int m_row;

	ResultSetBase(int fetchSize)
	{
		m_fetchSize = fetchSize;
		m_row = 0;	// First row is 1 so 0 is on undefined position.
	}

	public int getFetchDirection()
	throws SQLException
	{
		return FETCH_FORWARD;
	}

	public final int getFetchSize()
	throws SQLException
	{
		return m_fetchSize;
	}

	public final int getRow()
	throws SQLException
	{
		return m_row;
	}

	public int getType()
	throws SQLException
	{
		return TYPE_FORWARD_ONLY;
	}

	/**
	 * Cursor positoning is not implemented yet.
	 * @throws SQLException indicating that this feature is not supported.
	 */
	public void afterLast()
	throws SQLException
	{
		throw new UnsupportedFeatureException("Cursor positioning");
	}

	/**
	 * Cursor positoning is not implemented yet.
	 * @throws SQLException indicating that this feature is not supported.
	 */
	public void beforeFirst()
	throws SQLException
	{
		throw new UnsupportedFeatureException("Cursor positioning");
	}

	public void close()
	throws SQLException
	{
		m_row = -1;
	}

	public boolean isClosed()
	throws SQLException
	{
		return m_row < 0;
	}

	/**
	 * Cursor positioning is not implemented yet.
	 * @throws SQLException indicating that this feature is not supported.
	 */
	public boolean first() throws SQLException
	{
		throw new UnsupportedFeatureException("Cursor positioning");
	}

	public boolean isAfterLast() throws SQLException
	{
		return m_row < 0;
	}

	public boolean isBeforeFirst() throws SQLException
	{
		return m_row == 0;
	}

	public boolean isFirst() throws SQLException
	{
		return m_row == 1;
	}

	/**
	 * Cursor positioning is not implemented yet.
	 * @throws SQLException indicating that this feature is not supported.
	 */
	public boolean last()
	throws SQLException
	{
		throw new UnsupportedFeatureException("Cursor positioning");
	}

	/**
	 * Reverse positioning is not implemented yet.
	 * @throws SQLException indicating that this feature is not supported.
	 */
	public boolean previous()
	throws SQLException
	{
		throw new UnsupportedFeatureException("Reverse positioning");
	}

	/**
	 * Cursor positioning is not implemented yet.
	 * @throws SQLException indicating that this feature is not supported.
	 */
	public boolean absolute(int row)
	throws SQLException
	{
		throw new UnsupportedFeatureException("Cursor positioning");
	}

	/**
	 * Cursor positioning is not implemented yet.
	 * @throws SQLException indicating that this feature is not supported.
	 */
	public boolean relative(int rows)
	throws SQLException
	{
		throw new UnsupportedFeatureException("Cursor positioning");
	}

	/**
	 * Only {@link java.sql.ResultSet#FETCH_FORWARD} is supported.
	 * @throws SQLException indicating that this feature is not supported
	 * for other values on <code>direction</code>.
	 */
	public void setFetchDirection(int direction)
	throws SQLException
	{
		if(direction != FETCH_FORWARD)
			throw new UnsupportedFeatureException("Non forward fetch direction");
	}

	public void setFetchSize(int fetchSize)
	throws SQLException
	{
		if(fetchSize <= 0)
			throw new IllegalArgumentException("Illegal fetch size for ResultSet");
		m_fetchSize = fetchSize;
	}

	final void setRow(int row)
	{
		m_row = row;
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
