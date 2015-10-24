/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.internal;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.WeakHashMap;
import java.util.logging.Logger;

/**
 * @author Thomas Hallgren
 */
public class PgSavepoint implements java.sql.Savepoint
{
	private static final WeakHashMap s_knownSavepoints = new WeakHashMap();

	private long m_pointer;

	PgSavepoint(long pointer)
	{
		m_pointer = pointer;
	}

	public static PgSavepoint set(String name)
	throws SQLException
	{
		synchronized(Backend.THREADLOCK)
		{
			PgSavepoint sp = new PgSavepoint(_set(name));
			s_knownSavepoints.put(sp, Boolean.TRUE);
			return sp;
		}
	}

	static PgSavepoint forId(int savepointId)
	{
		if(savepointId != 0)
		{
			synchronized(Backend.THREADLOCK)
			{
				Iterator itor = s_knownSavepoints.keySet().iterator();
				while(itor.hasNext())
				{
					PgSavepoint sp = (PgSavepoint)itor.next();
					if(savepointId == _getId(sp.m_pointer))
						return sp;
				}
			}
		}
		return null;
	}

	public int hashCode()
	{
		return this.getSavepointId();
	}

	public boolean equals(Object o)
	{
		return (this == o);
	}

	public void release()
	throws SQLException
	{
		synchronized(Backend.THREADLOCK)
		{
			_release(m_pointer);
			s_knownSavepoints.remove(this);
			m_pointer = 0;
		}
	}

	public void rollback()
	throws SQLException
	{
		synchronized(Backend.THREADLOCK)
		{
			_rollback(m_pointer);
			s_knownSavepoints.remove(this);
			m_pointer = 0;
		}
	}

	public String getSavepointName() throws SQLException
	{
		synchronized(Backend.THREADLOCK)
		{
			return _getName(m_pointer);
		}
	}

	public int getSavepointId()
	{
		synchronized(Backend.THREADLOCK)
		{
			return _getId(m_pointer);
		}
	}

	public void onInvocationExit(Connection conn)
	throws SQLException
	{
		if(m_pointer == 0)
			return;

		Logger logger = Logger.getAnonymousLogger();
		if(Backend.isReleaseLingeringSavepoints())
		{
			logger.warning("Releasing savepoint '" + _getId(m_pointer) +
				"' since its lifespan exceeds that of the function where it was set");
			conn.releaseSavepoint(this);
		}
		else
		{
			logger.warning("Rolling back to savepoint '" + _getId(m_pointer) +
				"' since its lifespan exceeds that of the function where it was set");
			conn.rollback(this);
		}
	}

	private static native long _set(String name)
	throws SQLException;

	private static native void _release(long pointer)
	throws SQLException;

	private static native void _rollback(long pointer)
	throws SQLException;

	private static native String _getName(long pointer);

	private static native int _getId(long pointer);
}
