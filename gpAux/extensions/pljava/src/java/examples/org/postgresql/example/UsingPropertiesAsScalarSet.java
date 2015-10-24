/*
 * Copyright (c) 2004, 2005 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * This implementation uses another function that returns a set of a complex
 * type, concatenates the name and value of that type and returns this as
 * a set of a scalar type. Somewhat cumbersome way to display properties
 * but it's a good test.
 *
 * @author Thomas Hallgren
 */
public class UsingPropertiesAsScalarSet
{
	public static Iterator getProperties()
	throws SQLException
	{
		StringBuffer bld = new StringBuffer();
		ArrayList list = new ArrayList();
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		Statement  stmt = conn.createStatement();
		try
		{
			ResultSet rs = stmt.executeQuery("SELECT name, value FROM propertyExample()");
			try
			{
				while(rs.next())
				{
					bld.setLength(0);
					bld.append(rs.getString(1));
					bld.append(" = ");
					bld.append(rs.getString(2));
					list.add(bld.toString());
				}
				return list.iterator();
			}
			finally
			{
				try { rs.close(); } catch(SQLException e) {}
			}
		}
		finally
		{
			try { stmt.close(); } catch(SQLException e) {}
		}
	}
}
