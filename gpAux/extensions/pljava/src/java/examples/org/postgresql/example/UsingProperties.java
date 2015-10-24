/*
 * Copyright (c) 2004, 2005 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.example;

import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import org.postgresql.pljava.ObjectPool;
import org.postgresql.pljava.PooledObject;
import org.postgresql.pljava.ResultSetProvider;
import org.postgresql.pljava.SessionManager;

import java.net.*;

/**
 * @author Thomas Hallgren
 */
public class UsingProperties implements ResultSetProvider, PooledObject
{
	private static Logger s_logger = Logger.getAnonymousLogger();
	private final Properties m_properties;
	private final ObjectPool m_pool;

	private Iterator m_propertyIterator;

	public UsingProperties(ObjectPool pool)
	throws IOException
	{
		m_pool = pool;
		m_properties = new Properties();

                URL urlloc = this.getClass().getResource("example.properties");
                s_logger.info("example.properties location is " + urlloc);
		Package pack = this.getClass().getPackage();
		String packageName = pack.getName()+" "+pack.getImplementationTitle() + " " + pack.getImplementationVendor() + " " + pack.getImplementationVersion();
		s_logger.info("UsingProperties package name: " + packageName );

		s_logger.info("** UsingProperties()");
		InputStream propStream = this.getClass().getResourceAsStream("example.properties");
		if(propStream == null)
		{
			s_logger.info("example.properties was null");
		}
		else
		{
			m_properties.load(propStream);
			propStream.close();
			s_logger.info("example.properties has " + m_properties.size() + " entries");
		}
		pack = m_properties.getClass().getPackage();
		packageName = pack.getName()+" "+pack.getImplementationTitle() + " " + pack.getImplementationVendor() + " " + pack.getImplementationVersion();
		s_logger.info("m_properties package name: " + packageName );
		if (pack.isSealed())
		{
			s_logger.info("package sealed");
		}
	}

	public void activate()
	{
		s_logger.info("** UsingProperties.activate()");
		m_propertyIterator = m_properties.entrySet().iterator();
	}

	public void remove()
	{
		s_logger.info("** UsingProperties.remove()");
		m_properties.clear();
	}

	public void passivate()
	{
		s_logger.info("** UsingProperties.passivate()");
		m_propertyIterator = null;
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

	public void close()
	throws SQLException
	{
		m_pool.passivateInstance(this);
	}

	public static ResultSetProvider getProperties()
	throws SQLException
	{
		ObjectPool pool = SessionManager.current().getObjectPool(UsingProperties.class);
		return (ResultSetProvider)pool.activateInstance();
	}
}
