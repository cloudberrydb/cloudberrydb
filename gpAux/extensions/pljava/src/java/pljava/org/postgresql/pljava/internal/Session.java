/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.internal;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import org.postgresql.pljava.ObjectPool;
import org.postgresql.pljava.SavepointListener;
import org.postgresql.pljava.TransactionListener;
import org.postgresql.pljava.jdbc.SQLUtils;


/**
 * An instance of this interface reflects the current session. The attribute
 * store is transactional.
 *
 * @author Thomas Hallgren
 */
public class Session implements org.postgresql.pljava.Session
{
	private final TransactionalMap m_attributes = new TransactionalMap(new HashMap());

	/**
	 * Adds the specified listener to the list of listeners that will
	 * receive transactional events.
	 */
	public void addTransactionListener(TransactionListener listener)
	{
		XactListener.addListener(listener);
	}

	/**
	 * Adds the specified listener to the list of listeners that will
	 * receive savepoint events.
	 */
	public void addSavepointListener(SavepointListener listener)
	{
		SubXactListener.addListener(listener);
	}

	public Object getAttribute(String attributeName)
	{
		return m_attributes.get(attributeName);
	}

	public ObjectPool getObjectPool(Class cls)
	{
		return ObjectPoolImpl.getObjectPool(cls);
	}

	/**
	 * Return the current user.
	 */
	public String getUserName()
	{
		return AclId.getUser().getName();
	}

	/**
	 * Return the session user.
	 */
	public String getSessionUserName()
	{
		return AclId.getSessionUser().getName();
	}


	public void removeAttribute(String attributeName)
	{
		m_attributes.remove(attributeName);
	}

	public void setAttribute(String attributeName, Object value)
	{
		m_attributes.put(attributeName, value);
	}

	/**
	 * Removes the specified listener from the list of listeners that will
	 * receive transactional events.
	 */
	public void removeTransactionListener(TransactionListener listener)
	{
		XactListener.removeListener(listener);
	}

	/**
	 * Removes the specified listener from the list of listeners that will
	 * receive savepoint events.
	 */
	public void removeSavepointListener(SavepointListener listener)
	{
		SubXactListener.removeListener(listener);
	}

	public void executeAsSessionUser(Connection conn, String statement)
	throws SQLException
	{
		Statement stmt = conn.createStatement();
		synchronized(Backend.THREADLOCK)
		{
			ResultSet rs = null;
			AclId sessionUser = AclId.getSessionUser();
			AclId effectiveUser = AclId.getUser();
			try
			{
				_setUser(sessionUser);
				if(stmt.execute(statement))
				{
					rs = stmt.getResultSet();
					rs.next();
				}
			}
			finally
			{
				SQLUtils.close(rs);
				SQLUtils.close(stmt);
				_setUser(effectiveUser);
			}
		}
	}

	/**
	 * Called from native code when the JVM is instantiated.
	 */
	static long init()
	throws SQLException
	{
		ELogHandler.init();
		
		// Should be replace with a Thread.getId() once we abandon
		// Java 1.4
		//
		return System.identityHashCode(Thread.currentThread());
	}

	private static native void _setUser(AclId userId);
}
