/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root directory of this distribution or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.jdbc;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Ref;
import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.SQLOutput;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;

import org.postgresql.pljava.internal.Backend;

/**
 * The SQLOutputToChunk uses JNI to build a PostgreSQL StringInfo buffer in
 * memory. A user should never make an attempt to create an instance of this
 * class. Only internal JNI routines can do that. An instance is propagated
 * in a call from the internal JNI layer to the Java layer will only survive
 * during that single call. The handle of the instance will be invalidated when
 * the call returns and subsequent use of the instance will yield a
 * SQLException with the message "Stream is closed".
 *
 * @author Thomas Hallgren
 */
public class SQLOutputToChunk implements SQLOutput
{
	private static final byte[] s_byteBuffer = new byte[8];

	private long m_handle;

	public SQLOutputToChunk(long handle)
	{
		m_handle = handle;
	}

	public void writeArray(Array value) throws SQLException
	{
		throw new UnsupportedOperationException("writeArray");
	}

	public void writeAsciiStream(InputStream value) throws SQLException
	{
		throw new UnsupportedOperationException("writeAsciiStream");
	}

	public void writeBigDecimal(BigDecimal value) throws SQLException
	{
		this.writeString(value.toString());
	}

	public void writeBinaryStream(InputStream value) throws SQLException
	{
		byte[] buf = new byte[1024];
		ByteArrayOutputStream bld = new ByteArrayOutputStream();
		int count;
		try
		{
			while((count = value.read(buf)) > 0)
				bld.write(buf, 0, count);
			this.writeBytes(bld.toByteArray());
		}
		catch(IOException e)
		{
			throw new SQLException(e.getMessage());
		}
	}

	public void writeBlob(Blob value) throws SQLException
	{
		throw new UnsupportedOperationException("writeBlob");
	}

	public void writeBoolean(boolean value) throws SQLException
	{
		this.write(value ? 1 : 0);
	}

	public void writeByte(byte value) throws SQLException
	{
		this.write(value);
	}

	public void writeBytes(byte[] buffer) throws SQLException
	{
		int len = buffer.length;
		if(len > 0)
		{
			if(len > 65535)
				throw new SQLException("Byte buffer exceeds maximum size of 65535 bytes");

			synchronized(Backend.THREADLOCK)
			{
				if(m_handle == 0)
					throw new SQLException("Stream is closed");

				s_byteBuffer[0] = (byte)((len >> 8) & 0xff);
				s_byteBuffer[1] = (byte)(len & 0xff);
				_writeBytes(m_handle, s_byteBuffer, 2);
				_writeBytes(m_handle, buffer, len);
			}
		}
	}

	public void writeCharacterStream(Reader value) throws SQLException
	{
		char[] buf = new char[1024];
		StringWriter bld = new StringWriter();
		int count;
		try
		{
			while((count = value.read(buf)) > 0)
				bld.write(buf, 0, count);
			this.writeString(bld.toString());
		}
		catch(IOException e)
		{
			throw new SQLException(e.getMessage());
		}
	}

	public void writeClob(Clob value) throws SQLException
	{
		throw new UnsupportedOperationException("writeClob");
	}

	public void writeNClob(java.sql.NClob value) 
	throws SQLException
	{
		throw new UnsupportedOperationException("NClob not yet supported");
	}

	public void writeDate(Date value) throws SQLException
	{
		this.writeLong(value.getTime());
	}

	public void writeDouble(double value) throws SQLException
	{
		this.writeLong(Double.doubleToLongBits(value));
	}

	public void writeFloat(float value) throws SQLException
	{
		this.writeInt(Float.floatToIntBits(value));
	}

	public void writeInt(int value) throws SQLException
	{
		synchronized(Backend.THREADLOCK)
		{
			s_byteBuffer[0] = (byte)(value >>> 24);
			s_byteBuffer[1] = (byte)(value >>> 16);
			s_byteBuffer[2] = (byte)(value >>> 8);
			s_byteBuffer[3] = (byte)(value >>> 0);
			_writeBytes(m_handle, s_byteBuffer, 4);
		}
	}

	public void writeLong(long value) throws SQLException
	{
		synchronized(Backend.THREADLOCK)
		{
			s_byteBuffer[0] = (byte)(value >>> 56);
			s_byteBuffer[1] = (byte)(value >>> 48);
			s_byteBuffer[2] = (byte)(value >>> 40);
			s_byteBuffer[3] = (byte)(value >>> 32);
			s_byteBuffer[4] = (byte)(value >>> 24);
			s_byteBuffer[5] = (byte)(value >>> 16);
			s_byteBuffer[6] = (byte)(value >>> 8);
			s_byteBuffer[7] = (byte)(value >>> 0);
			_writeBytes(m_handle, s_byteBuffer, 8);
		}
	}

	public void writeObject(SQLData value) throws SQLException
	{
		throw new UnsupportedOperationException("writeObject");
	}

	public void writeRef(Ref value) throws SQLException
	{
		throw new UnsupportedOperationException("writeRef");
	}

	public void writeShort(short value) throws SQLException
	{
		synchronized(Backend.THREADLOCK)
		{
			s_byteBuffer[0] = (byte)(value >>> 8);
			s_byteBuffer[1] = (byte)(value >>> 0);
			_writeBytes(m_handle, s_byteBuffer, 2);
		}
	}

	public void writeString(String value) throws SQLException
	{
		try
		{
			this.writeBytes(value.getBytes("UTF8"));
		}
		catch(UnsupportedEncodingException e)
		{
			throw new SQLException("UTF8 encoding not supported by JVM");
		}
	}

	public void writeNString(String value)
	throws SQLException
	{
		throw new UnsupportedOperationException("NString not yet supported");
	}

	public void writeStruct(Struct value) throws SQLException
	{
		throw new UnsupportedOperationException("writeStruct");
	}

	public void writeTime(Time value) throws SQLException
	{
		this.writeLong(value.getTime());
	}

	public void writeTimestamp(Timestamp value) throws SQLException
	{
		this.writeLong(value.getTime());
	}

	public void writeURL(URL value) throws SQLException
	{
		this.writeString(value.toString());
	}

	public void writeRowId(java.sql.RowId value) throws SQLException
	{
		throw new UnsupportedOperationException("RowId not yet supported");
	}

	public void writeSQLXML(java.sql.SQLXML value) throws SQLException
	{
		throw new UnsupportedOperationException("SQLXML not yet supported");
	}

	void close()
	{
		m_handle = 0;
	}

	private void write(int b) throws SQLException
	{
		synchronized(Backend.THREADLOCK)
		{
			if(m_handle == 0)
				throw new SQLException("Stream is closed");
			_writeByte(m_handle, b);
		}
	}

	private static native void _writeByte(long handle, int theByte);
	private static native void _writeBytes(long handle, byte[] src, int len);
}
