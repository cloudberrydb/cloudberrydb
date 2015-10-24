/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.internal;

import java.sql.SQLException;

/**
 * The <code>Portal</code> correspons to the internal PostgreSQL
 * <code>Portal</code> type.
 *
 * @author Thomas Hallgren
 */
public class Portal
{
	private long m_pointer;

	Portal(long pointer)
	{
		m_pointer = pointer;
	}

	/**
	 * Invalidates this structure and frees up memory using the
	 * internal function <code>SPI_cursor_close</code>
	 */
	public void close()
	{
		synchronized(Backend.THREADLOCK)
		{
			_close(m_pointer);
			m_pointer = 0;
		}
	}

	/**
	 * Returns the name of this Portal.
	 * @throws SQLException if the handle to the native structur is stale.
	 */
	public String getName()
	throws SQLException
	{
		synchronized(Backend.THREADLOCK)
		{
			return _getName(m_pointer);
		}
	}

	/**
	 * Returns the value of the <code>portalPos</code> attribute.
	 * @throws SQLException if the handle to the native structur is stale.
	 */
	public int getPortalPos()
	throws SQLException
	{
		synchronized(Backend.THREADLOCK)
		{
			return _getPortalPos(m_pointer);
		}
	}

	/**
	 * Returns the TupleDesc that describes the row Tuples for this
	 * Portal.
	 * @throws SQLException if the handle to the native structur is stale.
	 */
	public TupleDesc getTupleDesc()
	throws SQLException
	{
		synchronized(Backend.THREADLOCK)
		{
			return _getTupleDesc(m_pointer);
		}
	}

	/**
	 * Performs an <code>SPI_cursor_fetch</code>.
	 * @param forward Set to <code>true</code> for forward, <code>false</code> for backward.
	 * @param count Maximum number of rows to fetch.
	 * @return The actual number of fetched rows.
	 * @throws SQLException if the handle to the native structur is stale.
	 */
	public int fetch(boolean forward, int count)
	throws SQLException
	{
		synchronized(Backend.THREADLOCK)
		{
			return _fetch(m_pointer, forward, count);
		}
	}

	/**
	 * Returns the value of the <code>atEnd</code> attribute.
	 * @throws SQLException if the handle to the native structur is stale.
	 */
	public boolean isAtEnd()
	throws SQLException
	{
		synchronized(Backend.THREADLOCK)
		{
			return _isAtEnd(m_pointer);
		}
	}

	/**
	 * Returns the value of the <code>atStart</code> attribute.
	 * @throws SQLException if the handle to the native structur is stale.
	 */
	public boolean isAtStart()
	throws SQLException
	{
		synchronized(Backend.THREADLOCK)
		{
			return _isAtStart(m_pointer);
		}
	}

	/**
	 * Returns the value of the <code>posOverflow</code> attribute.
	 * @throws SQLException if the handle to the native structur is stale.
	 */
	public boolean isPosOverflow()
	throws SQLException
	{
		synchronized(Backend.THREADLOCK)
		{
			return _isPosOverflow(m_pointer);
		}
	}

	/**
	 * Checks if the portal is still active. I can be closed either explicitly
	 * using the {@link #close()} mehtod or implicitly due to a pop of invocation
	 * context.
	 */
	public boolean isValid()
	{
		return m_pointer != 0;
	}

	/**
	 * Performs an <code>SPI_cursor_move</code>.
	 * @param forward Set to <code>true</code> for forward, <code>false</code> for backward.
	 * @param count Maximum number of rows to fetch.
	 * @return The value of the global variable <code>SPI_result</code>.
	 * @throws SQLException if the handle to the native structur is stale.
	 */
	public int move(boolean forward, int count)
	throws SQLException
	{
		synchronized(Backend.THREADLOCK)
		{
			return _move(m_pointer, forward, count);
		}
	}

	private static native String _getName(long pointer)
	throws SQLException;

	private static native int _getPortalPos(long pointer)
	throws SQLException;

	private static native TupleDesc _getTupleDesc(long pointer)
	throws SQLException;

	private static native int _fetch(long pointer, boolean forward, int count)
	throws SQLException;

	private static native void _close(long pointer);

	private static native boolean _isAtEnd(long pointer)
	throws SQLException;

	private static native boolean _isAtStart(long pointer)
	throws SQLException;
	
	private static native boolean _isPosOverflow(long pointer)
	throws SQLException;

	private static native int _move(long pointer, boolean forward, int count)
	throws SQLException;
}
