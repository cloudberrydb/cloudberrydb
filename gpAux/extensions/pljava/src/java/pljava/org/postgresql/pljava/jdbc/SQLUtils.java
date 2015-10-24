/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Helpful utility commands when dealing with JDBC
 * @author Thomas Hallgren
 */
public abstract class SQLUtils
{
	public static Connection getDefaultConnection()
	{
		return SPIDriver.getDefault();
	}

	public static void close(Connection conn)
	{
		if(conn != null)
			try
			{
				conn.close();
			}
			catch(SQLException e)
			{ /* ignore */
			}
	}

	public static void close(Statement stmt)
	{
		if(stmt != null)
			try
			{
				stmt.close();
			}
			catch(SQLException e)
			{ /* ignore */
			}
	}

	public static void close(ResultSet rs)
	{
		if(rs != null)
			try
			{
				rs.close();
			}
			catch(SQLException e)
			{ /* ignore */
			}
	}

}
