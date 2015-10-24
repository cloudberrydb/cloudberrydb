/*
 * Copyright (c) 2004, 2005 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.example;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.postgresql.pljava.ResultSetHandle;

/**
 * This implementation uses another function that returns a set of a complex
 * type and returns the ResultSet produced by a query.
 *
 * @author Thomas Hallgren
 */
public class UsingPropertiesAsResultSet implements ResultSetHandle
{
	private PreparedStatement m_statement;

	public ResultSet getResultSet() throws SQLException
	{
		m_statement = DriverManager.getConnection("jdbc:default:connection").prepareStatement("SELECT * FROM propertyExample()");
		return m_statement.executeQuery();
	}

	public void close()
	throws SQLException
	{
		m_statement.close();
		m_statement = null;
	}

	public static ResultSetHandle getProperties()
	throws SQLException
	{
		return new UsingPropertiesAsResultSet();
	}
}
