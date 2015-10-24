/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.jdbc;

import java.sql.SQLException;
import java.util.ArrayList;
import java.sql.NClob;

import org.postgresql.pljava.internal.Tuple;
import org.postgresql.pljava.internal.TupleDesc;

/**
 * A single row, updateable ResultSet specially made for triggers. The
 * changes made to this ResultSet are remembered and converted to a
 * SPI_modify_tuple call prior to function return.
 *
 * @author Thomas Hallgren
 */
public class TriggerResultSet extends SingleRowResultSet
{
	private ArrayList m_tupleChanges;
	private final TupleDesc m_tupleDesc;
	private final Tuple     m_tuple;
	private final boolean   m_readOnly;

	public TriggerResultSet(TupleDesc tupleDesc, Tuple tuple, boolean readOnly)
	throws SQLException
	{
		m_tupleDesc = tupleDesc;
		m_tuple = tuple;
		m_readOnly = readOnly;
	}

	/**
	 * Cancel all changes made to the Tuple.
	 */
	public void cancelRowUpdates()
	throws SQLException
	{
		m_tupleChanges = null;
	}

	/**
	 * Cancels all changes but doesn't really close the set.
	 */
	public void close()
	throws SQLException
	{
		m_tupleChanges = null;
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

	/**
	 * Returns the concurrency for this ResultSet.
	 * @see java.sql.ResultSet#getConcurrency
	 */
	public int getConcurrency() throws SQLException
	{
		return m_readOnly ? CONCUR_READ_ONLY : CONCUR_UPDATABLE;
	}

	/**
	 * Returns <code>true</code> if this row has been updated.
	 */
	public boolean rowUpdated()
	throws SQLException
	{
		return m_tupleChanges != null;
	}

	/**
	 * Store this change for later use
	 */
	public void updateObject(int columnIndex, Object x)
	throws SQLException
	{
		if(m_readOnly)
			throw new UnsupportedFeatureException("ResultSet is read-only");

		if(m_tupleChanges == null)
			m_tupleChanges = new ArrayList();

		m_tupleChanges.add(new Integer(columnIndex));
		m_tupleChanges.add(x);
	}
	
	/**
	 * Return a 2 element array describing the changes that has been made to
	 * the contained Tuple. The first element is an <code>int[]</code> containing
	 * the index of each changed value. The second element is an <code>Object[]
	 * </code> with containing the corresponding values.
	 * 
	 * @return The 2 element array or <code>null</code> if no change has been made.
	 */
	public Object[] getChangeIndexesAndValues()
	{
		ArrayList changes = m_tupleChanges;
		if(changes == null)
			return null;

		int top = changes.size();
		if(changes.size() == 0)
			return null;

		top /= 2;
		int[] indexes = new int[top];
		Object[] values = new Object[top];
		int vIdx = 0;
		for(int idx = 0; idx < top; ++idx)
		{	
			indexes[idx] = ((Integer)changes.get(vIdx++)).intValue();
			values[idx] = changes.get(vIdx++);
		}
		return new Object[] { m_tuple, indexes, values };
	}

	protected Object getObjectValue(int columnIndex)
	throws SQLException
	{
		// Check if this value has been changed.
		//
		ArrayList changes = m_tupleChanges;
		if(changes != null)
		{	
			int top = changes.size();
			for(int idx = 0; idx < top; idx += 2)
				if(columnIndex == ((Integer)changes.get(idx)).intValue())
					return changes.get(idx + 1);
		}
		return m_tuple.getObject(this.getTupleDesc(), columnIndex);
	}

	protected final TupleDesc getTupleDesc()
	{
		return m_tupleDesc;
	}
}
