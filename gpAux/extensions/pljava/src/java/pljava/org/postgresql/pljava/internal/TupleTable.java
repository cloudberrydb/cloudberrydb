/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.internal;

/**
 * The <code>SPITupleTable</code> correspons to the internal PostgreSQL
 * <code>SPITupleTable</code> type.
 *
 * @author Thomas Hallgren
 */
public class TupleTable
{
	private final TupleDesc m_tupleDesc;
	private final Tuple[] m_tuples;

	TupleTable(TupleDesc tupleDesc, Tuple[] tuples)
	{
		m_tupleDesc = tupleDesc;
		m_tuples = tuples;
	}

	public final TupleDesc getTupleDesc()
	{
		return m_tupleDesc;
	}

	/**
	 * Returns the number of <code>Tuple</code> instances contained in this table.
	 */
	public final int getCount()
	{
		return m_tuples.length;
	}

	/**
	 * Returns the <code>Tuple</code> at the given index.
	 * @param position Index of desired slot. First slot has index zero. 
	 */
	public final Tuple getSlot(int position)
	{
		return m_tuples[position];
	}
}
