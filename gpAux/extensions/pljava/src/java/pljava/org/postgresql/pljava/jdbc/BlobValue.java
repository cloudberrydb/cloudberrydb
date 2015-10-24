/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.jdbc;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.sql.Blob;
import java.sql.SQLException;

/**
 * @author Thomas Hallgren
 */
public class BlobValue extends InputStream implements Blob
{
	public static int getStreamLength(InputStream value) throws SQLException
	{
		try
		{
			value.mark(Integer.MAX_VALUE);
			long length = value.skip(Long.MAX_VALUE);
			if(length > Integer.MAX_VALUE)
				throw new SQLException("stream content too large");
			value.reset();
			return (int)length;
		}
		catch(IOException e)
		{
			throw new SQLException(e.getMessage());
		}
	}

	private long m_markPos;
	private final long m_nBytes;
	private final InputStream m_stream;

	private long m_streamPos;
	private boolean m_free;

	public BlobValue(byte[] bytes)
	{
		this(new ByteArrayInputStream(bytes), bytes.length);
	}

	public BlobValue(InputStream stream)
	throws SQLException
	{
		m_stream    = stream;
		m_nBytes    = getStreamLength(stream);
		m_streamPos = 0L;
		m_markPos   = 0L;
		m_free      = false;
	}

	public BlobValue(InputStream stream, int nBytes)
	{
		m_stream    = stream;
		m_nBytes    = (long) nBytes;
		m_streamPos = 0L;
		m_markPos   = 0L;
		m_free      = false;
	}

	public BlobValue(InputStream stream, long nBytes)
	{
		m_stream    = stream;
		m_nBytes    = nBytes;
		m_streamPos = 0L;
		m_markPos   = 0L;
		m_free      = false;
	}

	/**
	 * This method frees the Blob object and releases the resources that it
	 * holds. The object is invalid once the free method is called.
	 *
	 * After free has been called, any attempt to invoke a method other than
	 * free will result in a SQLException being thrown. If free is called
	 * multiple times, the subsequent calls to free are treated as a no-op.
	*/
	public void free()
	throws SQLException
	{
		if (!m_free)
		{
			try 
			{
				m_stream.close();
			}
			catch(IOException e)
			{
				throw new SQLException("Error closing Blob stream: " + e.getMessage());
			}
		}

		m_free = true;
	}

	//***************************************
	// Implementation of java.io.InputStream
	//***************************************
	public int available()
	throws IOException
	{
		return m_stream.available();
	}

	public InputStream getBinaryStream()
	{
		return this;
	}

	public InputStream getBinaryStream(long pos, long length)
    throws SQLException
	{
		return new BlobValue(this.getBytes(pos, length));
	}

	public byte[] getBytes(long pos, int length)
	throws SQLException
	{
		if(pos < 0L || length < 0)
			throw new IllegalArgumentException();
		if(length == 0)
			return new byte[0];

		if(pos + length > m_nBytes)
			throw new SQLException("Attempt to read beyond end of Blob data");

		long skip = pos - m_streamPos;
		if(skip < 0)
			throw new SQLException("Cannot position Blob stream backwards");

		try
		{
			if(skip > 0)
				this.skip(skip);

			byte[] buf = new byte[length];
			this.read(buf);
			return buf;
		}
		catch(IOException e)
		{
			throw new SQLException("Error reading Blob data: " + e.getMessage());
		}
	}

	public byte[] getBytes(long pos, long length)
	throws SQLException
	{
		if (length > Integer.MAX_VALUE)
			throw new SQLException("buffer length too large");
		return this.getBytes(pos, (int) length);
	}


	/**
	 * Called from within...
	 * @param buf a buffer that reflects the internally allocated bytea buffer.
	 * This size of this buffer will be exactly the size returned by a call to
	 * {@link #length()}.
	 * @throws IOException
	 */
	public void getContents(ByteBuffer buf)
	throws IOException
	{
		int rs = 0;
		if(buf.hasArray())
		{
			byte[] bytes = buf.array();
			rs = m_stream.read(bytes);
		}
		else
		{
			byte[] trBuf = new byte[1024];
			int br;
			while((br = m_stream.read(trBuf)) > 0)
			{
				buf.put(trBuf, 0, br);
				rs += br;
			}
		}
		if(rs != m_nBytes)
			throw new IOException("Not all bytes could be read");
		m_streamPos += rs;
	}

	//***************************************
	// Implementation of java.sql.Blob
	//***************************************
	public long length()
	throws SQLException
	{
		if (m_free)
			throw new SQLException("Illegal access to freed BlobValue");

		return m_nBytes;
	}

	public synchronized void mark(int readLimit)
	{
		m_stream.mark(readLimit);
		m_markPos = m_streamPos;
	}

	public boolean markSupported()
	{
		return m_stream.markSupported();
	}

	/**
	 * Not supported.
	 */
	public long position(Blob pattern, long start)
	{
		throw new UnsupportedOperationException();
	}

	/**
	 * Not supported.
	 */
	public long position(byte[] pattern, long start)
	{
		throw new UnsupportedOperationException();
	}

	public synchronized int read()
	throws IOException
	{
		int rs = m_stream.read();
		m_streamPos++;
		return rs;
	}

	public synchronized int read(byte[] b)
	throws IOException
	{
		int rs = m_stream.read(b);
		m_streamPos += rs;
		return rs;
	}

	public synchronized int read(byte[] b, int off,  int len)
	throws IOException
	{
		int rs = m_stream.read(b, off, len);
		m_streamPos += rs;
		return rs;
	}

	public synchronized void reset()
	throws IOException
	{
		m_stream.reset();
		m_streamPos = m_markPos;
	}

	public synchronized long skip(long nBytes)
	throws IOException
	{
		long skipped = m_stream.skip(nBytes);
		m_streamPos += skipped;
		return skipped;
	}

	//*************************************************************************
	// Implementation of java.sql.Blob JDK 1.4 methods
	//
	// Those method are intended to provide a channel to the underlying data
	// storage as an alternative to the setBinaryStream
	// on the preparedStatement and are not implemented by the BlobValue.
	//
	//*************************************************************************
	/**
	 * In this method is not supported by <code>BlobValue</code>
	 */
	public OutputStream setBinaryStream(long pos)
	{
		throw new UnsupportedOperationException();
	}

	/**
	 * In this method is not supported by <code>BlobValue</code>
	 */
	public int setBytes(long pos, byte[] bytes)
	{
		throw new UnsupportedOperationException();
	}

	/**
	 * In this method is not supported by <code>BlobValue</code>
	 */
	public int setBytes(long pos, byte[] bytes, int offset, int len)
	{
		throw new UnsupportedOperationException();
	}

	/**
	 * In this method is not supported by <code>BlobValue</code>
	 */
	public void truncate(long len)
	{
		throw new UnsupportedOperationException();
	}	
}
