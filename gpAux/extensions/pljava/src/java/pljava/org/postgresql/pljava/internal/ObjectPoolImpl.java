/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root directory of this distribution or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.internal;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.IdentityHashMap;

import org.postgresql.pljava.ObjectPool;
import org.postgresql.pljava.PooledObject;

class ObjectPoolImpl implements ObjectPool
{
	/**
	 * An InstanceHandle is a link in a single linked list that
	 * holds on to a ResultSetProvider.
	 */
	private static class PooledObjectHandle
	{
		private PooledObject m_instance;
		private PooledObjectHandle m_next;
	}

	private static Class[] s_ctorSignature = { ObjectPool.class };
	private static PooledObjectHandle s_handlePool;
	private static final IdentityHashMap s_poolCache = new IdentityHashMap();

	private final Constructor m_ctor;
	private PooledObjectHandle m_providerPool;

	private ObjectPoolImpl(Class c)
	{
		if(!PooledObject.class.isAssignableFrom(c))
			throw new IllegalArgumentException("Class " + c + " does not implement the " +
				PooledObject.class + " interface");

		try
		{
			m_ctor = c.getConstructor(s_ctorSignature);
		}
		catch(SecurityException e)
		{
			throw new RuntimeException(e);
		}
		catch(NoSuchMethodException e)
		{
			throw new IllegalArgumentException("Unable to locate constructor " + c + "(ObjectPool)");
		}
	}

	/**
	 * Obtain a pool for the given class.
	 * @param cls
	 * @return
	 * @throws SQLException
	 */
	public static ObjectPoolImpl getObjectPool(Class cls)
	{
		ObjectPoolImpl pool = (ObjectPoolImpl)s_poolCache.get(cls);
		if(pool == null)
		{
			pool = new ObjectPoolImpl(cls);
			s_poolCache.put(cls, pool);
		}
		return pool;
	}

	public PooledObject activateInstance()
	throws SQLException
	{
		PooledObject instance;
		PooledObjectHandle handle = m_providerPool;
		if(handle != null)
		{
			m_providerPool = handle.m_next;
			instance = handle.m_instance;

			// Return the handle to the unused handle pool.
			//
			handle.m_instance = null;
			handle.m_next = s_handlePool;
			s_handlePool = handle;
		}
		else
		{
			try
			{
				instance = (PooledObject)m_ctor.newInstance(new Object[] { this });
			}
			catch(InvocationTargetException e)
			{
				Throwable t = e.getTargetException();
				if(t instanceof SQLException)
					throw (SQLException)t;
				if(t instanceof RuntimeException)
					throw (RuntimeException)t;
				if(t instanceof Error)
					throw (Error)t;
				throw new SQLException(e.getMessage());
			}
			catch(RuntimeException e)
			{
				throw e;
			}
			catch(Exception e)
			{
				throw new SQLException("Failed to create an instance of: " +
					m_ctor.getDeclaringClass() + " :" + e.getMessage());
			}
		}
		try
		{
			instance.activate();
		}
		catch(SQLException e)
		{
			instance.remove();
			throw e;
		}
		return instance;
	}

	public void passivateInstance(PooledObject instance)
	throws SQLException
	{
		try
		{
			instance.passivate();
		}
		catch(SQLException e)
		{
			instance.remove();
			throw e;
		}

		// Obtain a handle from the pool of handles so that
		// we have something to wrap the instance in.
		//
		PooledObjectHandle handle = s_handlePool;
		if(handle != null)
			s_handlePool = handle.m_next;
		else
			handle = new PooledObjectHandle();

		handle.m_instance = instance;
		handle.m_next = m_providerPool;
		m_providerPool = handle;
	}

	public void removeInstance(PooledObject instance) throws SQLException
	{
		PooledObjectHandle prev = null;
		for(PooledObjectHandle handle = m_providerPool; handle != null; handle = handle.m_next)
		{
			if(handle.m_instance == instance)
			{
				if(prev == null)
					m_providerPool = handle.m_next;
				else
					prev.m_next = handle.m_next;

				handle.m_instance = null;
				handle.m_next = s_handlePool;
				s_handlePool = handle;
				break;
			}
		}
		instance.remove();
	}
}
