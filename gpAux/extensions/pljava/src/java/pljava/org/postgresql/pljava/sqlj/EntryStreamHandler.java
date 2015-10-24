/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.sqlj;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.postgresql.pljava.jdbc.SQLUtils;

/**
 * @author Thomas Hallgren
 */
class EntryStreamHandler extends URLStreamHandler
{
	private static EntryStreamHandler s_instance;

	protected URLConnection openConnection(URL u)
	throws IOException
	{
		return new EntryConnection(u);
	}

	/**
	 * Creates an EntryStreamHandler on the first invocation. This handler is
	 * then cached and returned on subsequent invocations.
	 * @return the one and only handler.
	 */
	static URLStreamHandler getInstance()
	{
		if(s_instance == null)
			s_instance = new EntryStreamHandler();
		return s_instance;
	}

	class EntryConnection extends URLConnection
	{
		private final int m_entryId;
		private String m_entryName;
		private byte[] m_image;

		protected EntryConnection(URL entryURL)
		{
			super(entryURL);
			m_entryId = Integer.parseInt(url.getPath().substring(1));
		}

		/**
		 * Executes the prepared statement that will fetch an image based on
		 * the entryId denoted by the URL of this connection.
		 * If connect has been called already, subsequent calls are ignored.
		 */
		public void connect()
		throws IOException
		{
			if(connected)
				return;

			PreparedStatement stmt = null;
    		ResultSet rs = null;
	    	try
			{

				stmt = SQLUtils.getDefaultConnection().prepareStatement(
					"SELECT entryName, entryImage FROM sqlj.jar_entry WHERE entryId = ?");
				stmt.setInt(1, m_entryId);
	    		rs = stmt.executeQuery();
				if(rs.next())
				{
					m_entryName = rs.getString(1);
					m_image = rs.getBytes(2);
					connected = true;
				}
				else
					throw new FileNotFoundException("jarId = " + m_entryId);
			}
	    	catch(SQLException e)
			{
	    		throw new IOException(e.getMessage());
			}
			finally
			{
				SQLUtils.close(rs);
				SQLUtils.close(stmt);
			}
		}

		/**
		 * Creates a new {@link java.io.ByteArrayInputStream} for the image that
		 * was obtained during {@link #connect}.
		 * @return the created input stream.
		 */
	    public InputStream getInputStream()
	    throws IOException
		{
	    	this.connect();
			return new ByteArrayInputStream(m_image);
	    }

	    /**
	     * Consults the current {@link java.net.FileNameMap FileNameMap} to obtain
	     * the MIME type for the file name extension for the entry handled by this
	     * connection.
	     * @return the content type based on the file name extension.
	     */
	    public String getContentType()
		{
	    	return getFileNameMap().getContentTypeFor(m_entryName);
		}
	}
}


