/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.internal;

import java.sql.SQLException;

/**
 * The <code>TupleDesc</code> correspons to the internal PostgreSQL
 * <code>TupleDesc</code>.
 *
 * @author Thomas Hallgren
 */
public class TupleDesc extends JavaWrapper
{
	private final int m_size;
	private Class[] m_columnClasses;

	TupleDesc(long pointer, int size) throws SQLException
	{
		super(pointer);
		m_size = size;
	}

	/**
	 * Returns the name of the column at <code>index</code>.
	 * @param index The one based index of the column.
	 * @return The name of the column.
	 * @throws SQLException If the index is out of range for this
	 * tuple descriptor.
	 */
	public String getColumnName(int index)
	throws SQLException
	{
		synchronized(Backend.THREADLOCK)
		{
			return _getColumnName(this.getNativePointer(), index);
		}
	}

	/**
	 * Returns the index of the column named <code>colName</code>.
	 * @param colName The name of the column.
	 * @return The index for column <code>colName</code>.
	 * @throws SQLException If no column with the given name can
	 * be found in this tuple descriptor.
	 */
	public int getColumnIndex(String colName)
	throws SQLException
	{
		synchronized(Backend.THREADLOCK)
		{
			return _getColumnIndex(this.getNativePointer(), colName.toLowerCase());
		}
	}

	/**
	 * Creates a <code>Tuple</code> that is described by this descriptor and
	 * initialized with the supplied <code>values</code>.
	 * @return The created <code>Tuple</code>.
	 * @throws SQLException If the length of the values array does not
	 * match the size of the descriptor or if the handle of this descriptor
	 * has gone stale.
	 */
	public Tuple formTuple(Object[] values)
	throws SQLException
	{
		synchronized(Backend.THREADLOCK)
		{
			return _formTuple(this.getNativePointer(), values);
		}
	}

	/**
	 * Returns the number of columns in this tuple descriptor.
	 */
	public int size()
	{
		return m_size;
	}

	/**
	 * Returns the Java class of the column at index
	 */
	public Class getColumnClass(int index)
	throws SQLException
	{
		if(m_columnClasses == null)
		{
			m_columnClasses = new Class[m_size];
			synchronized(Backend.THREADLOCK)
			{				
				long _this = this.getNativePointer();
				for(int idx = 0; idx < m_size; ++idx)
					m_columnClasses[idx] = _getOid(_this, idx+1).getJavaClass();
			}
		}
		return m_columnClasses[index-1];
	}

	/**
	 * Returns OID of the column type.
	 */
	public Oid getOid(int index)
	throws SQLException
	{
		synchronized(Backend.THREADLOCK)
		{
			return _getOid(this.getNativePointer(), index);
		}
	}

	/**
	 * Calls the backend function FreeTupleDesc(TupleDesc desc)
	 * @param pointer The native pointer to the source TupleDesc
	 */
	protected native void _free(long pointer);

	private static native String _getColumnName(long _this, int index) throws SQLException;
	private static native int _getColumnIndex(long _this, String colName) throws SQLException;
	private static native Tuple _formTuple(long _this, Object[] values) throws SQLException;
	private static native Oid _getOid(long _this, int index) throws SQLException;
}
