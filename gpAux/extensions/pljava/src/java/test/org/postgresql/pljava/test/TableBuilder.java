/*
 * Copyright (c) 2004 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root directory of this distribution or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class TableBuilder
{
	private final Connection m_conn;

	public static void main(String[] args)
	{
		try
		{
			TableBuilder t = new TableBuilder();
			t.createTable();
			t.createJavaFunction();
			t.doSelects();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}

	public TableBuilder()
	throws Exception
	{
		Class.forName("org.postgresql.Driver");
		m_conn = DriverManager.getConnection("jdbc:postgresql://localhost/thhal", "thhal", null);
	}
	
	public void createTable()
	throws Exception
	{
		Statement stmt = m_conn.createStatement();
		try
		{
			stmt.executeUpdate("drop table xtable");
		}
		catch(SQLException e)
		{}

		stmt.executeUpdate("create table xtable (cl int4, tstove varchar)");
		
		PreparedStatement insert = m_conn.prepareStatement("INSERT INTO xtable(cl, tstove) VALUES (?, ?)");
		for(int idx = 0; idx < 100000; ++idx)
		{
			insert.setInt(1, idx);
			insert.setString(2, "xyz_" + (idx % 100));
			insert.executeUpdate();
		}
		insert.close();
		m_conn.commit();
	}

	public void createJavaFunction()
	throws Exception
	{
		Statement stmt = m_conn.createStatement();
		stmt.executeUpdate("CREATE OR REPLACE FUNCTION getstovename(character varying, character varying) RETURNS text" +
				" AS $$java.lang.System.getProperty$$" +
				" LANGUAGE java");
	}

	public void doSelects()
	throws Exception
	{
		Statement stmt = m_conn.createStatement();
		int cnt = 0;
		ResultSet rs = stmt.executeQuery("SELECT getstovename('my.non.property', xtable.tstove) FROM xtable");
		while(rs.next())
		{
			String tstove = rs.getString(1);
			if(!tstove.startsWith("xyz_"))
				throw new Exception("Tstove was " + tstove);
			++cnt;
		}
		rs.close();
		System.out.println("Succesfully read " + cnt + " rows");
	}
}
