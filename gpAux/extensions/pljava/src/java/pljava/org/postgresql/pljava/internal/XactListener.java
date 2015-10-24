/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.internal;

import java.sql.SQLException;
import java.util.HashMap;

import org.postgresql.pljava.TransactionListener;


/**
 * Class that enables registrations using the PostgreSQL <code>RegisterXactCallback</code>
 * function.
 *
 * @author Thomas Hallgren
 */
class XactListener
{
	private static final HashMap s_listeners = new HashMap();

	static void onAbort(long listenerId) throws SQLException
	{
		TransactionListener listener = (TransactionListener)s_listeners.get(new Long(listenerId));
		if(listener != null)
			listener.onAbort(Backend.getSession());
	}

	static void onCommit(long listenerId) throws SQLException
	{
		TransactionListener listener = (TransactionListener)s_listeners.get(new Long(listenerId));
		if(listener != null)
			listener.onCommit(Backend.getSession());
	}

	static void onPrepare(long listenerId) throws SQLException
	{
		TransactionListener listener = (TransactionListener)s_listeners.get(new Long(listenerId));
		if(listener != null)
			listener.onPrepare(Backend.getSession());
	}
	
	static void addListener(TransactionListener listener)
	{
		synchronized(Backend.THREADLOCK)
		{
			long key = System.identityHashCode(listener);
			if(s_listeners.put(new Long(key), listener) != listener)
				_register(key);
		}
	}
	
	static void removeListener(TransactionListener listener)
	{
		synchronized(Backend.THREADLOCK)
		{
			long key = System.identityHashCode(listener);
			if(s_listeners.remove(new Long(key)) == listener)
				_unregister(key);
		}
	}

	private static native void _register(long listenerId);

	private static native void _unregister(long listenerId);
}
