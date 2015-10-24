/*
 * Copyright (c) 2004 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root directory of this distribution or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.example;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.postgresql.pljava.ResultSetHandle;

public class Users implements ResultSetHandle
{
	private final String m_filter;
	private Statement m_statement;

	public Users(String filter)
	{
		m_filter = filter;
	}

	public ResultSet getResultSet()
	throws SQLException
	{
		m_statement = DriverManager.getConnection("jdbc:default:connection").createStatement();
		return m_statement.executeQuery("SELECT * FROM pg_user WHERE " + m_filter);
	}

	public void close()
	throws SQLException
	{
		m_statement.close();
	}

	public static ResultSetHandle listSupers()
	{
		return new Users("usesuper = true");
	}

	public static ResultSetHandle listNonSupers()
	{
		return new Users("usesuper = false");
	}
}
