/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root directory of this distribution or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.tasks;

import java.sql.Connection;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.taskdefs.JDBCTask;

public abstract class PLJavaTask extends JDBCTask
{
	private String m_hostName;
	private String m_database;
	private int m_portNumber = -1;

	public PLJavaTask()
	{
		this.setDriver("org.postgresql.Driver");
	}

	public final String getDatabase()
	{
		return m_database == null ? this.getUserId() : m_database;
	}

	public final void setDatabase(String database)
	{
		m_database = database;
	}

	public final String getHostName()
	{
		return m_hostName == null ? "localhost" : m_hostName;
	}

	public final void setHostName(String hostName)
	{
		m_hostName = hostName;
	}

	public final int getPortNumber()
	{
		return m_portNumber;
	}

	public final void setPortNumber(int portNumber)
	{
		m_portNumber = portNumber;
	}

    protected Connection getConnection()
	throws BuildException
	{
		String url = this.getUrl();
		if(url == null)
		{
			StringBuffer cc = new StringBuffer();
			cc.append("jdbc:postgresql://");
			cc.append(this.getHostName());
			int port = this.getPortNumber();
			if(port != -1)
			{	
				cc.append(':');
				cc.append(port);
			}
			cc.append('/');
			cc.append(this.getDatabase());
			this.setUrl(cc.toString());
		}
		return super.getConnection();
	}
}
