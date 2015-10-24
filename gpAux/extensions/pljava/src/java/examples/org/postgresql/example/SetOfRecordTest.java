/*
 * Copyright (c) 2004 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root directory of this distribution or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.postgresql.pljava.ResultSetHandle;

public class SetOfRecordTest implements ResultSetHandle
{
	private final PreparedStatement m_statement;

	public static ResultSetHandle executeSelect(String selectSQL)
	throws SQLException
	{
		return new SetOfRecordTest(selectSQL);
	}

	public SetOfRecordTest(String selectSQL) throws SQLException
	{
		Connection conn = DriverManager
			.getConnection("jdbc:default:connection");
		m_statement = conn.prepareStatement(selectSQL);
	}

	public void close() throws SQLException
	{
		m_statement.close();
	}

	public ResultSet getResultSet() throws SQLException
	{
		return m_statement.executeQuery();
	}
}
