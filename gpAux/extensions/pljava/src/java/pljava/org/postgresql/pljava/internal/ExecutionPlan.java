/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.internal;

import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.LinkedHashMap;

/**
 * The <code>ExecutionPlan</code> correspons to the execution plan obtained
 * using an internal PostgreSQL <code>SPI_prepare</code> call.
 * 
 * @author Thomas Hallgren
 */
public class ExecutionPlan
{
	static final int INITIAL_CACHE_CAPACITY = 29;

	static final float CACHE_LOAD_FACTOR = 0.75f;

	private long m_pointer;

	/**
	 * MRU cache for prepared plans.
	 */
	static final class PlanCache extends LinkedHashMap
	{
		private final int m_cacheSize;

		public PlanCache(int cacheSize)
		{
			super(INITIAL_CACHE_CAPACITY, CACHE_LOAD_FACTOR, true);
			m_cacheSize = cacheSize;
		}

		protected boolean removeEldestEntry(Map.Entry eldest)
		{
			if(this.size() <= m_cacheSize)
				return false;

			ExecutionPlan evicted = (ExecutionPlan)eldest.getValue();
			synchronized(Backend.THREADLOCK)
			{
				if(evicted.m_pointer != 0)
				{
					_invalidate(evicted.m_pointer);
					evicted.m_pointer = 0;
				}
			}
			return true;
		}
	};

	static final class PlanKey
	{
		private final int m_hashCode;

		private final String m_stmt;

		private final Oid[] m_argTypes;

		PlanKey(String stmt, Oid[] argTypes)
		{
			m_stmt = stmt;
			m_hashCode = stmt.hashCode() + 1;
			m_argTypes = argTypes;
		}

		public boolean equals(Object o)
		{
			if(!(o instanceof PlanKey))
				return false;

			PlanKey pk = (PlanKey)o;
			if(!pk.m_stmt.equals(m_stmt))
				return false;

			Oid[] pat = pk.m_argTypes;
			Oid[] mat = m_argTypes;
			int idx = pat.length;
			if(mat.length != idx)
				return false;

			while(--idx >= 0)
				if(!pat[idx].equals(mat[idx]))
					return false;

			return true;
		}

		public int hashCode()
		{
			return m_hashCode;
		}
	}

	private static final Map s_planCache;

	private final Object m_key;

	static
	{
		int cacheSize = Backend.getStatementCacheSize();
		s_planCache = Collections.synchronizedMap(new PlanCache(cacheSize < 11
			? 11
			: cacheSize));
	}

	private ExecutionPlan(Object key, long pointer)
	{
		m_key = key;
		m_pointer = pointer;
	}

	/**
	 * Close the plan.
	 */
	public void close()
	{
		ExecutionPlan old = (ExecutionPlan)s_planCache.put(m_key, this);
		if(old != null && old.m_pointer != 0)
		{
			synchronized(Backend.THREADLOCK)
			{
				_invalidate(old.m_pointer);
				old.m_pointer = 0;
			}
		}
	}

	/**
	 * Set up a cursor that will execute the plan using the internal
	 * <code>SPI_cursor_open</code> function
	 * 
	 * @param cursorName Name of the cursor or <code>null</code> for a system
	 *            generated name.
	 * @param parameters Values for the parameters.
	 * @return The <code>Portal</code> that represents the opened cursor.
	 * @throws SQLException If the underlying native structure has gone stale.
	 */
	public Portal cursorOpen(String cursorName, Object[] parameters)
	throws SQLException
	{
		synchronized(Backend.THREADLOCK)
		{
			return _cursorOpen(m_pointer, System.identityHashCode(Thread
				.currentThread()), cursorName, parameters);
		}
	}

	/**
	 * Checks if this <code>ExecutionPlan</code> can create a <code>Portal
	 * </code>
	 * using {@link #cursorOpen}. This is true if the plan contains only one
	 * regular <code>SELECT</code> query.
	 * 
	 * @return <code>true</code> if the plan can create a <code>Portal</code>
	 * @throws SQLException If the underlying native structure has gone stale.
	 */
	public boolean isCursorPlan() throws SQLException
	{
		synchronized(Backend.THREADLOCK)
		{
			return _isCursorPlan(m_pointer);
		}
	}

	/**
	 * Execute the plan using the internal <code>SPI_execp</code> function.
	 * 
	 * @param parameters Values for the parameters.
	 * @param rowCount The maximum number of tuples to create. A value of
	 *            <code>rowCount</code> of zero is interpreted as no limit,
	 *            i.e., run to completion.
	 * @return One of the status codes declared in class {@link SPI}.
	 * @throws SQLException If the underlying native structure has gone stale.
	 */
	public int execute(Object[] parameters, int rowCount) throws SQLException
	{
		synchronized(Backend.THREADLOCK)
		{
			return _execute(m_pointer, System.identityHashCode(Thread
				.currentThread()), parameters, rowCount);
		}
	}

	/**
	 * Create an execution plan for a statement to be executed later using the
	 * internal <code>SPI_prepare</code> function.
	 * 
	 * @param statement The command string.
	 * @param argTypes SQL types of argument types.
	 * @return An execution plan for the prepared statement.
	 * @throws SQLException
	 * @see java.sql.Types
	 */
	public static ExecutionPlan prepare(String statement, Oid[] argTypes)
	throws SQLException
	{
		Object key = (argTypes == null)
			? (Object)statement
			: (Object)new PlanKey(statement, argTypes);

		ExecutionPlan plan = (ExecutionPlan)s_planCache.remove(key);
		if(plan == null)
		{
			synchronized(Backend.THREADLOCK)
			{
				plan = new ExecutionPlan(key, _prepare(
					System.identityHashCode(Thread.currentThread()), statement, argTypes));
			}
		}
		return plan;
	}

	private static native Portal _cursorOpen(long pointer, long threadId,
		String cursorName, Object[] parameters) throws SQLException;

	private static native boolean _isCursorPlan(long pointer)
	throws SQLException;

	private static native int _execute(long pointer, long threadId,
		Object[] parameters, int rowCount) throws SQLException;

	private static native long _prepare(long threadId, String statement, Oid[] argTypes)
	throws SQLException;

	private static native void _invalidate(long pointer);
}
