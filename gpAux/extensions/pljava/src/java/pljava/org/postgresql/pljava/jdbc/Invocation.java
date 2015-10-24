/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Logger;

import org.postgresql.pljava.internal.Backend;
import org.postgresql.pljava.internal.PgSavepoint;

/**
 * @author Thomas Hallgren
 */
public class Invocation
{
	/**
	 * The current "stack" of invocations.
	 */
	private static Invocation[] s_levels = new Invocation[10];

	/**
	 * Nesting level for this invocation
	 */
	private final int m_nestingLevel;

	/**
	 * Top level savepoint relative to this invocation.
	 */
	private PgSavepoint m_savepoint;

	private Invocation(int level)
	{
		m_nestingLevel = level;
	}

	/**
	 * @return The nesting level of this invocation
	 */
	public int getNestingLevel()
	{
		return m_nestingLevel;
	}

	/**
	 * @return Returns the savePoint.
	 */
	final PgSavepoint getSavepoint()
	{
		return m_savepoint;
	}

	private ArrayList m_preparedStatements;

	final void manageStatement(PreparedStatement statement)
	{
		if(m_preparedStatements == null)
			m_preparedStatements = new ArrayList();
		m_preparedStatements.add(statement);
	}

	final void forgetStatement(PreparedStatement statement)
	{
		if(m_preparedStatements == null)
			return;

		int idx = m_preparedStatements.size();
		while(--idx >= 0)
			if(m_preparedStatements.get(idx) == statement)
			{
				m_preparedStatements.remove(idx);
				return;
			}
	}

	/**
	 * @param savepoint The savepoint to set.
	 */
	final void setSavepoint(PgSavepoint savepoint)
	{
		m_savepoint = savepoint;
	}

	/**
	 * Called from the backend when the invokation exits. Should
	 * not be invoked any other way.
	 */
	public void onExit()
	throws SQLException
	{
		try
		{
			if(m_savepoint != null)
				m_savepoint.onInvocationExit(SPIDriver.getDefault());

			if(m_preparedStatements != null)
			{
				int idx = m_preparedStatements.size();
				if(idx > 0)
				{
					Logger w = Logger.getAnonymousLogger();
					w.warning(
						"Closing " + idx + " \"forgotten\" statement"
							+ ((idx > 1) ? "s" : ""));
					while(--idx >= 0)
					{
						PreparedStatement stmt = (PreparedStatement)m_preparedStatements.get(idx);
						w.fine("Closed: " + stmt);
						stmt.close();
					}
				}
			}
		}
		finally
		{
			s_levels[m_nestingLevel] = null;
		}
	}

	/**
	 * @return The current invocation
	 */
	public static Invocation current()
	{
		synchronized(Backend.THREADLOCK)
		{
			Invocation curr = _getCurrent();
			if(curr != null)
				return curr;

			int level = _getNestingLevel();
			int top = s_levels.length;
			if(level < top)
			{
				curr = s_levels[level];
				if(curr != null)
				{
					curr._register();
					return curr;
				}
			}
			else
			{
				int newSize = top;
				do { newSize <<= 2; } while(newSize <= level);
				Invocation[] levels = new Invocation[newSize];
				System.arraycopy(s_levels, 0, levels, 0, top);
				s_levels = levels;
			}
			curr = new Invocation(level);
			s_levels[level] = curr;
			curr._register();
			return curr;
		}
	}

	static void clearErrorCondition()
	{
		synchronized(Backend.THREADLOCK)
		{
			_clearErrorCondition();
		}
	}

	/**
	 * Register this Invocation so that it receives the onExit callback
	 */
	private native void  _register();
	
	/**
	 * Returns the current invocation or null if no invocation has been
	 * registered yet.
	 */
	private native static Invocation  _getCurrent();

	/**
	 * Returns the current nesting level
	 */
	private native static int  _getNestingLevel();
	
	/**
	 * Clears the error condition set by elog(ERROR)
	 */
	private native static void  _clearErrorCondition();
}
