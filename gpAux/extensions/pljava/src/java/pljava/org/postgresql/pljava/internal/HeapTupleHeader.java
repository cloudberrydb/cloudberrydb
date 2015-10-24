/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.internal;

import java.sql.SQLException;

/**
 * The <code>HeapTupleHeader</code> correspons to the internal PostgreSQL
 * <code>HeapTupleHeader</code> struct.
 *
 * @author Thomas Hallgren
 */
public class HeapTupleHeader extends JavaWrapper
{
	private final TupleDesc m_tupleDesc;

	HeapTupleHeader(long pointer, TupleDesc tupleDesc)
	{
		super(pointer);
		m_tupleDesc = tupleDesc;
	}

	/**
	 * Obtains a value from the underlying native <code>HeapTupleHeader</code>
	 * structure.
	 * @param index Index of value in the structure (one based).
	 * @return The value or <code>null</code>.
	 * @throws SQLException If the underlying native structure has gone stale.
	 */
	public final Object getObject(int index)
	throws SQLException
	{
		synchronized(Backend.THREADLOCK)
		{
			return _getObject(this.getNativePointer(), m_tupleDesc.getNativePointer(), index);
		}
	}

	/**
	 * Obtains the TupleDesc that describes the tuple and returns it.
	 * @return The TupleDesc that describes this tuple.
	 */
	public final TupleDesc getTupleDesc()
	{
		return m_tupleDesc;
	}

	protected native void _free(long pointer);

	private static native Object _getObject(long pointer, long tupleDescPointer, int index)
	throws SQLException;
}
