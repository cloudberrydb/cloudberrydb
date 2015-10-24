/*
 * Copyright (c) 2004, 2005 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.example;

import java.sql.*;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.logging.Logger;

import org.postgresql.pljava.ResultSetProvider;

/**
 * @author Filip Hrbek
 */
public class MetaDataBooleans implements ResultSetProvider
{
	String[] methodNames;

	Boolean[] methodResults;

	public MetaDataBooleans() throws SQLException
	{
		Logger log = Logger.getAnonymousLogger();

		class MethodComparator implements Comparator
		{
			public int compare(Object a, Object b)
			{
				return ((Method)a).getName().compareTo(((Method)b).getName());
			}
		}

		Connection conn = DriverManager
			.getConnection("jdbc:default:connection");
		DatabaseMetaData md = conn.getMetaData();
		Method[] m = DatabaseMetaData.class.getMethods();
		Arrays.sort(m, new MethodComparator());
		Class prototype[];
		Class returntype;
		Object[] args = new Object[0];
		Boolean result = null;
		ArrayList mn = new ArrayList();
		ArrayList mr = new ArrayList();

		for(int i = 0; i < m.length; i++)
		{
			prototype = m[i].getParameterTypes();
			if(prototype.length > 0)
				continue;

			returntype = m[i].getReturnType();
			if(!returntype.equals(boolean.class))
				continue;

			try
			{
				result = (Boolean)m[i].invoke(md, args);
			}
			catch(Exception e)
			{
				log.info("Method: " + m[i].getName() + " => " + e.getMessage());
			}
                      catch(AbstractMethodError e)
                      {
                              // probably a JDBC 4 method that isn't supported yet
                              log.info("Method: " + m[i].getName() + " => " + e.getMessage());
                      }

			mn.add(m[i].getName());
			mr.add(result);
		}

		methodNames = (String[])mn.toArray(new String[0]);
		methodResults = (Boolean[])mr.toArray(new Boolean[0]);
	}

	public boolean assignRowValues(ResultSet receiver, int currentRow)
	throws SQLException
	{
		if(currentRow < methodNames.length)
		{
			receiver.updateString(1, methodNames[currentRow]);
			receiver.updateBoolean(2, methodResults[currentRow].booleanValue());
			return true;
		}
		return false;
	}

	public void close()
	{
	}

	public static ResultSetProvider getDatabaseMetaDataBooleans()
	throws SQLException
	{
		try
		{
			return new MetaDataBooleans();
		}
		catch(SQLException e)
		{
			throw new SQLException("Error reading DatabaseMetaData", e
				.getMessage());
		}
	}
}
