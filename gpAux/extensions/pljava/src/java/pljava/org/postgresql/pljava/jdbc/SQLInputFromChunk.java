/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root directory of this distribution or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.jdbc;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Ref;
import java.sql.SQLException;
import java.sql.SQLInput;
import java.sql.Time;
import java.sql.Timestamp;

import org.postgresql.pljava.internal.Backend;

/**
 * The SQLInputToChunk uses JNI to read from memory that has been allocated by
 * the PostgreSQL backend. A user should never make an attempt to create an
 * instance of this class. Only internal JNI routines can do that. An instance
 * is propagated in a call from the internal JNI layer to the Java layer will
 * only survive during that single call. The handle of the instance will be
 * invalidated when the call returns and subsequent use of the instance will
 * yield a SQLException with the message "Stream is closed".
 * 
 * @author Thomas Hallgren
 */
public class SQLInputFromChunk implements SQLInput
{
	private static final byte[] s_byteBuffer = new byte[8];

	private final int m_chunkSize;

	private int m_position;

	private long m_handle;

	public SQLInputFromChunk(long handle, int chunkSize)
	{
		m_handle = handle;
		m_chunkSize = chunkSize;
		m_position = 0;
	}

	public Array readArray() throws SQLException
	{
		throw new UnsupportedOperationException("readArray");
	}

	public InputStream readAsciiStream() throws SQLException
	{
		throw new UnsupportedOperationException("readAsciiStream");
	}

	public BigDecimal readBigDecimal() throws SQLException
	{
		return new BigDecimal(this.readString());
	}

	public InputStream readBinaryStream() throws SQLException
	{
		return new ByteArrayInputStream(this.readBytes());
	}

	public Blob readBlob() throws SQLException
	{
		throw new UnsupportedOperationException("readBlob");
	}

	public boolean readBoolean() throws SQLException
	{
		int c = this.read();
		if(c < 0)
		    throw new SQLException("Unexpected EOF on data input");
		return c != 0;
	}

	public byte readByte() throws SQLException
	{
		int c = this.read();
		if(c < 0)
		    throw new SQLException("Unexpected EOF on data input");
		return (byte)c;
	}

	public byte[] readBytes() throws SQLException
	{
		synchronized(Backend.THREADLOCK)
		{
			if(m_handle == 0)
				throw new SQLException("Stream is closed");

			if(m_chunkSize - m_position < 2)
			    throw new SQLException("Unexpected EOF on data input");
			_readBytes(m_handle, m_position, s_byteBuffer, 2);
			m_position += 2;

		    int len = ((s_byteBuffer[0] & 0xff) <<  8) | (s_byteBuffer[1] & 0xff);
		    byte[] buffer = new byte[len];
		    if(len > 0)
		    {
		    	_readBytes(m_handle, m_position, buffer, len);
		    	m_position += len;
		    }
			return buffer;
		}
	}

	public Reader readCharacterStream() throws SQLException
	{
		return new StringReader(this.readString());
	}

	public Clob readClob() throws SQLException
	{
		throw new UnsupportedOperationException("readClob");
	}

	public java.sql.NClob readNClob()
	throws SQLException
	{
		throw new UnsupportedOperationException("readNClob");
	}

	public Date readDate() throws SQLException
	{
		return new Date(this.readLong());
	}

	public double readDouble() throws SQLException
	{
		return Double.longBitsToDouble(this.readLong());
	}

	public float readFloat() throws SQLException
	{
		return Float.intBitsToFloat(readInt());
	}

	public int readInt() throws SQLException
	{
		synchronized(Backend.THREADLOCK)
		{
			if(m_chunkSize - m_position < 4)
			    throw new SQLException("Unexpected EOF on data input");
			_readBytes(m_handle, m_position, s_byteBuffer, 4);
			m_position += 4;
	        return	((s_byteBuffer[0] & 0xff) << 24) |
		            ((s_byteBuffer[1] & 0xff) << 16) |
		            ((s_byteBuffer[2] & 0xff) <<  8) |
		             (s_byteBuffer[3] & 0xff);
		}
	}

	public long readLong() throws SQLException
	{
		synchronized(Backend.THREADLOCK)
		{
			if(m_chunkSize - m_position < 8)
			    throw new SQLException("Unexpected EOF on data input");
			_readBytes(m_handle, m_position, s_byteBuffer, 8);
			m_position += 8;
	        return	((long)(s_byteBuffer[0] & 0xff) << 56) |
		            ((long)(s_byteBuffer[1] & 0xff) << 48) |
		            ((long)(s_byteBuffer[2] & 0xff) << 40) |
		            ((long)(s_byteBuffer[3] & 0xff) << 32) |
	        		((long)(s_byteBuffer[4] & 0xff) << 24) |
		            	((s_byteBuffer[5] & 0xff) << 16) |
		            	((s_byteBuffer[6] & 0xff) <<  8) |
		            	 (s_byteBuffer[7] & 0xff);
		}
	}

	public Object readObject() throws SQLException
	{
		throw new UnsupportedOperationException("readObject");
	}

	public Ref readRef() throws SQLException
	{
		throw new UnsupportedOperationException("readRef");
	}

	public short readShort() throws SQLException
	{
		synchronized(Backend.THREADLOCK)
		{
			if(m_chunkSize - m_position < 2)
			    throw new SQLException("Unexpected EOF on data input");
			_readBytes(m_handle, m_position, s_byteBuffer, 2);
			m_position += 2;
	        return (short)(((s_byteBuffer[0] & 0xff) <<  8) | (s_byteBuffer[1] & 0xff));
		}
	}

	public String readString() throws SQLException
	{
		try
		{
			return new String(this.readBytes(), "UTF8");
		}
		catch(UnsupportedEncodingException e)
		{
			throw new SQLException("UTF8 encoding not supported by JVM");
		}
	}

	public String readNString() 
    throws SQLException
	{
		throw new UnsupportedOperationException("NString not yet supported");
	}

	public Time readTime() throws SQLException
	{
		return new Time(this.readLong());
	}

	public Timestamp readTimestamp() throws SQLException
	{
		return new Timestamp(this.readLong());
	}

	public URL readURL() throws SQLException
	{
		try
		{
			return new URL(this.readString());
		}
		catch(MalformedURLException e)
		{
			throw new SQLException(e.getMessage());
		}
	}

	public java.sql.RowId readRowId()
	throws SQLException
	{
		throw new UnsupportedOperationException("RowId not yet supported");
	}

	public java.sql.SQLXML readSQLXML()
	throws SQLException
	{
		throw new UnsupportedOperationException("SQLXML not yet supported");
	}

	public boolean wasNull() throws SQLException
	{
		return false;
	}

	void close()
	{
		m_handle = 0;
	}

	private int read() throws SQLException
	{
		if(m_position < m_chunkSize)
		{
			synchronized(Backend.THREADLOCK)
			{
				if(m_handle == 0)
					throw new SQLException("Stream is closed");
				return _readByte(m_handle, m_position++);
			}
		}
		return -1;
	}

	private static native int _readByte(long handle, int position);

	private static native void _readBytes(long handle, int position, byte[] dest, int len);
}
