/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;

/**
 * The SessionManager makes the current {@link Session} available to the
 * caller.
 * @author Thomas Hallgren
 */
public class SessionManager
{
	private static Method s_getSession;

	/**
	 * Returns the current session.
	 */
	public static Session current()
	throws SQLException
	{
		try
		{
			if(s_getSession == null)
			{
					String sp = System.getProperty(
									"org.postgresql.pljava.sessionprovider",
									"org.postgresql.pljava.internal.Backend");
					Class spc = Class.forName(sp);
					s_getSession = spc.getMethod("getSession", null);
			}
			return (Session)s_getSession.invoke(null, null);
		}
		catch (RuntimeException e)
		{
			throw e;
		}
		catch (InvocationTargetException e)
		{
			Throwable t = e.getTargetException();
			if(t instanceof SQLException)
				throw (SQLException)t;
			if(t instanceof RuntimeException)
				throw (RuntimeException)t;
			throw new SQLException(t.getMessage());
		}
		catch (Exception e)
		{
			throw new SQLException(e.getMessage());
		}
	}
}
