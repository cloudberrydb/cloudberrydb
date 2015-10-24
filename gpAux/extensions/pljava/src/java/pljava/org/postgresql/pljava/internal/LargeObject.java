/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.internal;

import java.sql.SQLException;

/**
 * The <code>LargeObject</code> correspons to the internal PostgreSQL
 * <code>LargeObjectDesc</code>.
 * 
 * @author Thomas Hallgren
 */
public class LargeObject extends JavaWrapper
{
	/**
	 *	Write mode flag to be passed to {@link #create} and {@link #open}
	 */
	public static final int INV_WRITE = 0x00020000;

	/**
	 *	Read mode flag to be passed to {@link #create} and {@link #open}
	 */
	public static final int INV_READ  = 0x00040000;

	/**
	 * Flag returned by {@link #create} and {@link #open}
	 */
	public static final int IFS_RDLOCK = (1 << 0);

	/**
	 * Flag returned by {@link #create} and {@link #open}
	 */
	public static final int IFS_WRLOCK = (1 << 1);

	/**
	 * Flag to be passed to {@link #seek} denoting that the
	 * offset parameter should be treated as an absolute address.
	 */
	public static final int SEEK_SET = 0;

	/**
	 * Flag to be passed to {@link #seek} denoting that the
	 * offset parameter should be treated relative to the current
	 * address.
	 */
	public static final int SEEK_CUR = 1;

	/**
	 * Flag to be passed to {@link #seek} denoting that the
	 * offset parameter should be treated relative to the end
	 * of the data.
	 */
	public static final int SEEK_END = 2;

	LargeObject(long nativePointer)
	{
		super(nativePointer);
	}

	/**
	 * Creates a LargeObject handle and returns the {@link Oid} of
	 * that handle.
	 * @param flags Flags to use for creation.
	 * @return A Oid that can be used in a call to {@link #open(Oid, int)}
	 * or {@link #drop(Oid)}.
	 * @throws SQLException
	 */
	public static Oid create(int flags)
	throws SQLException
	{
		synchronized(Backend.THREADLOCK)
		{
			return _create(flags);
		}
	}

	public static LargeObject open(Oid lobjId, int flags)
	throws SQLException
	{
		synchronized(Backend.THREADLOCK)
		{
			return _open(lobjId, flags);
		}
	}

	public void close()
	throws SQLException
	{
		synchronized(Backend.THREADLOCK)
		{
			_close(this.getNativePointer());
		}
	}

	public static int drop(Oid lobjId)
	throws SQLException
	{
		synchronized(Backend.THREADLOCK)
		{
			return _drop(lobjId);
		}
	}

	public Oid getId()
	throws SQLException
	{
		synchronized(Backend.THREADLOCK)
		{
			return _getId(this.getNativePointer());
		}
	}

	public long length()
	throws SQLException
	{
		synchronized(Backend.THREADLOCK)
		{
			return _length(this.getNativePointer());
		}
	}

	public long seek(long offset, int whence)
	throws SQLException
	{
		synchronized(Backend.THREADLOCK)
		{
			return _seek(this.getNativePointer(), offset, whence);
		}
	}

	public long tell()
	throws SQLException
	{
		synchronized(Backend.THREADLOCK)
		{
			return _tell(this.getNativePointer());
		}
	}

	public int read(byte[] buf)
	throws SQLException
	{
		synchronized(Backend.THREADLOCK)
		{
			return _read(this.getNativePointer(), buf);
		}
	}

	public int write(byte[] buf)
	throws SQLException
	{
		synchronized(Backend.THREADLOCK)
		{
			return _write(this.getNativePointer(), buf);
		}
	}

	private static native Oid _create(int flags)
	throws SQLException;

	private static native int _drop(Oid lobjId)
	throws SQLException;

	private static native LargeObject _open(Oid lobjId, int flags)
	throws SQLException;

	private static native void _close(long pointer)
	throws SQLException;

	private static native Oid _getId(long pointer)
	throws SQLException;

	private static native long _length(long pointer)
	throws SQLException;

	private static native long _seek(long pointer, long offset, int whence)
	throws SQLException;

	private static native long _tell(long pointer)
	throws SQLException;

	private static native int _read(long pointer, byte[] buf)
	throws SQLException;

	private static native int _write(long pointer, byte[] buf)
	throws SQLException;
}
