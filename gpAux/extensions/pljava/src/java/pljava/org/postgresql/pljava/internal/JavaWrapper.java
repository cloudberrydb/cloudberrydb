/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root directory of this distribution or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.internal;

public abstract class JavaWrapper
{
	private final long m_pointer;

	/**
	 * Creates an instance of this class that will be attached to a native
	 * structure represented by pointer. This constructor must only be called
	 * from native code.
	 * 
	 * @param pointer The wapped pointer.
	 */
	protected JavaWrapper(long pointer)
	{
		m_pointer = pointer;
	}

	public void finalize()
	{
		synchronized(Backend.THREADLOCK)
		{
			_free(m_pointer);
		}
	}

	/**
	 * Returns the native pointer
	 */
	public final long getNativePointer()
	{
		return m_pointer;
	}

	/**
	 * Calls the C function pfree() with the given pointer as an argument.
	 * Subclasses may override this method if special handling is needed when
	 * freeing up the object.
	 * 
	 * @param pointer The pointer to free.
	 */
	protected native void _free(long pointer);
}
