/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.internal;

import java.sql.SQLException;

/**
 * The <code>Tuple</code> correspons to the internal PostgreSQL
 * <code>HeapTuple</code>.
 *
 * @author Thomas Hallgren
 */
public class Tuple extends JavaWrapper
{
	Tuple(long pointer)
	{
		super(pointer);
	}

	/**
	 * Obtains a value from the underlying native <code>HeapTuple</code>
	 * structure.
	 * @param tupleDesc The Tuple descriptor for this instance.
	 * @param index Index of value in the structure (one based).
	 * @return The value or <code>null</code>.
	 * @throws SQLException If the underlying native structure has gone stale.
	 */
	public Object getObject(TupleDesc tupleDesc, int index)
	throws SQLException
	{
		synchronized(Backend.THREADLOCK)
		{
			return _getObject(this.getNativePointer(), tupleDesc.getNativePointer(), index);
		}
	}

	/**
	 * Calls the backend function heap_freetuple(HeapTuple tuple)
	 * @param pointer The native pointer to the source HeapTuple
	 */
	protected native void _free(long pointer);

	private static native Object _getObject(long pointer, long tupleDescPointer, int index)
	throws SQLException;
}
