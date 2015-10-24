/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.internal;

import java.sql.SQLException;
import java.util.HashMap;

import org.postgresql.pljava.SavepointListener;


/**
 * Class that enables registrations using the PostgreSQL <code>RegisterSubXactCallback</code>
 * function.
 *
 * @author Thomas Hallgren
 */
class SubXactListener
{
	private static final HashMap s_listeners = new HashMap();

	static void onAbort(long listenerId, int spId, int parentSpId) throws SQLException
	{
		SavepointListener listener = (SavepointListener)s_listeners.get(new Long(listenerId));
		if(listener != null)
			listener.onAbort(Backend.getSession(), PgSavepoint.forId(spId), PgSavepoint.forId(parentSpId));
	}

	static void onCommit(long listenerId, int spId, int parentSpId) throws SQLException
	{
		SavepointListener listener = (SavepointListener)s_listeners.get(new Long(listenerId));
		if(listener != null)
			listener.onCommit(Backend.getSession(), PgSavepoint.forId(spId), PgSavepoint.forId(parentSpId));
	}

	static void onStart(long listenerId, long spPointer, int parentSpId) throws SQLException
	{
		SavepointListener listener = (SavepointListener)s_listeners.get(new Long(listenerId));
		if(listener != null)
			listener.onStart(Backend.getSession(), new PgSavepoint(spPointer), PgSavepoint.forId(parentSpId));
	}

	static void addListener(SavepointListener listener)
	{
		synchronized(Backend.THREADLOCK)
		{
			long key = System.identityHashCode(listener);
			if(s_listeners.put(new Long(key), listener) != listener)
				_register(key);
		}
	}

	static void removeListener(SavepointListener listener)
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
