/*
 * Copyright (c) 2004, 2005 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.example.annotation;

import org.postgresql.pljava.annotation.Function;

import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import org.postgresql.pljava.ResultSetProvider;

/**
 * @author Thomas Hallgren
 */
public class UsingProperties implements ResultSetProvider
{
	private static Logger s_logger = Logger.getAnonymousLogger();
	private final Iterator m_propertyIterator;
	
	public UsingProperties()
	throws IOException
	{
		Properties v = new Properties();
		InputStream propStream = this.getClass().getResourceAsStream("example.properties");
		if(propStream == null)
		{
			s_logger.fine("example.properties was null");
			m_propertyIterator = Collections.EMPTY_SET.iterator();
		}
		else
		{
			v.load(propStream);
			propStream.close();
			s_logger.fine("example.properties has " + v.size() + " entries");
			m_propertyIterator = v.entrySet().iterator();
		}
	}

	public boolean assignRowValues(ResultSet receiver, int currentRow)
			throws SQLException
	{
		if(!m_propertyIterator.hasNext())
		{
			s_logger.fine("no more rows, returning false");
			return false;
		}
		Map.Entry propEntry = (Map.Entry)m_propertyIterator.next();
		receiver.updateString(1, (String)propEntry.getKey());
		receiver.updateString(2, (String)propEntry.getValue());
		// s_logger.fine("next row created, returning true");
		return true;
	}

	@Function( complexType = "javatest._properties")
	public static ResultSetProvider getProperties()
	throws SQLException
	{
		try
		{
			return new UsingProperties();
		}
		catch(IOException e)
		{
			throw new SQLException("Error reading properties", e.getMessage());
		}
	}

	public void close()
	{
	}
}
