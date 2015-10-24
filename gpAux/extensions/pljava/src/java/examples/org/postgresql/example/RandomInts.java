/*
 * Copyright (c) 2004 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root directory of this distribution or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.example;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Random;

public class RandomInts implements Iterator
{
	private final int m_rowCount;
	private final Random m_random;
	private int m_currentRow;

	public static Iterator createIterator(int rowCount)
	throws SQLException
	{
		return new RandomInts(rowCount);
	}

	public RandomInts(int rowCount) throws SQLException
	{
		m_rowCount = rowCount;
		m_random = new Random(System.currentTimeMillis());
	}

	public boolean hasNext()
	{
		return m_currentRow < m_rowCount;
	}

	public Object next()
	{
		if(m_currentRow < m_rowCount)
		{
			++m_currentRow;
			return new Integer(m_random.nextInt());
		}
		throw new NoSuchElementException();
	}

	public void remove()
	{
		throw new UnsupportedOperationException();
	}
}
