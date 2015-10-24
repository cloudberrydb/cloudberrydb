/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.postgresql.pljava.internal.Backend;
import org.postgresql.pljava.internal.TupleDesc;

/**
 * A single row, read-only ResultSet, specially made for functions and
 * procedures that takes complex types as arguments (PostgreSQL 7.5
 * and later).
 *
 * @author Thomas Hallgren
 */
public class SingleRowReader extends SingleRowResultSet
{
	private final TupleDesc m_tupleDesc;
	private final long m_pointer;

	public SingleRowReader(long pointer, TupleDesc tupleDesc)
	throws SQLException
	{
		m_pointer = pointer;
		m_tupleDesc = tupleDesc;
	}

	public void close()
	{
	}

	/**
	 * Retrieves whether this ResultSet object has been closed. A ResultSet is
	 * closed if the method close has been called on it, or if it is
	 * automatically closed.
	 *
	 * Note: Since close() doesn't really close the set the set is always open.
	 */
	public boolean isClosed()
	throws SQLException
	{
		return false;
	}

	public void finalize()
	{
		synchronized(Backend.THREADLOCK)
		{
			_free(m_pointer);
		}
	}

	protected Object getObjectValue(int columnIndex)
	throws SQLException
	{
		synchronized(Backend.THREADLOCK)
		{
			return _getObject(m_pointer, m_tupleDesc.getNativePointer(), columnIndex);
		}
	}

	/**
	 * Returns {@link ResultSet#CONCUR_READ_ONLY}.
	 */
	public int getConcurrency()
	throws SQLException
	{
		return ResultSet.CONCUR_READ_ONLY;
	}

	/**
	 * This feature is not supported on a <code>ReadOnlyResultSet</code>.
	 * @throws SQLException indicating that this feature is not supported.
	 */
	public void cancelRowUpdates()
	throws SQLException
	{
		throw readOnlyException();
	}

	/**
	 * This feature is not supported on a <code>ReadOnlyResultSet</code>.
	 * @throws SQLException indicating that this feature is not supported.
	 */
	public void deleteRow()
	throws SQLException
	{
		throw readOnlyException();
	}

	/**
	 * This feature is not supported on a <code>ReadOnlyResultSet</code>.
	 * @throws SQLException indicating that this feature is not supported.
	 */
	public void insertRow()
	throws SQLException
	{
		throw readOnlyException();
	}

	/**
	 * This feature is not supported on a <code>ReadOnlyResultSet</code>.
	 * @throws SQLException indicating that this feature is not supported.
	 */
	public void moveToInsertRow()
	throws SQLException
	{
		throw readOnlyException();
	}

	/**
	 * This feature is not supported on a <code>ReadOnlyResultSet</code>.
	 * @throws SQLException indicating that this feature is not supported.
	 */
	public void updateRow()
	throws SQLException
	{
		throw readOnlyException();
	}

	/**
	 * Always returns false.
	 */
	public boolean rowUpdated()
	throws SQLException
	{
		return false;
	}

	/**
	 * This feature is not supported on a <code>ReadOnlyResultSet</code>.
	 * @throws SQLException indicating that this feature is not supported.
	 */
	public void updateObject(int columnIndex, Object x) throws SQLException
	{
		throw readOnlyException();
	}

	/**
	 * This feature is not supported on a <code>ReadOnlyResultSet</code>.
	 * @throws SQLException indicating that this feature is not supported.
	 */
	public void updateObject(int columnIndex, Object x, int scale)
	throws SQLException
	{
		throw readOnlyException();
	}

	private static SQLException readOnlyException()
	{
		return new UnsupportedFeatureException("ResultSet is read-only");
	}

	protected final TupleDesc getTupleDesc()
	{
		return m_tupleDesc;
	}

	protected native void _free(long pointer);

	private static native Object _getObject(long pointer, long tupleDescPointer, int index)
	throws SQLException;
}
