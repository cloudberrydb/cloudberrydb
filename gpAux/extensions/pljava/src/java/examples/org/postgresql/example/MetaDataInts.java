/*
 * Copyright (c) 2004, 2005 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.example;

import java.sql.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.logging.Logger;

import org.postgresql.pljava.ResultSetProvider;

/**
 * @author Filip Hrbek
 */
public class MetaDataInts implements ResultSetProvider
{
	String[] methodNames;

	Integer[] methodResults;

	public MetaDataInts() throws SQLException
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
		Integer result = null;
		ArrayList mn = new ArrayList();
		ArrayList mr = new ArrayList();

		for(int i = 0; i < m.length; i++)
		{
			prototype = m[i].getParameterTypes();
			if(prototype.length > 0)
				continue;

			returntype = m[i].getReturnType();
			if(!returntype.equals(int.class))
				continue;

			try
			{
				result = (Integer)m[i].invoke(md, args);
			}
			catch(InvocationTargetException e)
			{
				log.info("Method: " + m[i].getName() + " => " + e.getTargetException().getMessage());
				result = new Integer(-1);
			}
			catch(Exception e)
			{
				log.info("Method: " + m[i].getName() + " => " + e.getMessage());
				result = new Integer(-1);
			}

			mn.add(m[i].getName());
			mr.add(result);
		}

		methodNames = (String[])mn.toArray(new String[mn.size()]);
		methodResults = (Integer[])mr.toArray(new Integer[mr.size()]);
	}

	public boolean assignRowValues(ResultSet receiver, int currentRow)
	throws SQLException
	{
		if(currentRow < methodNames.length)
		{
			receiver.updateString(1, methodNames[currentRow]);
			receiver.updateInt(2, methodResults[currentRow].intValue());
			return true;
		}
		return false;
	}

	public void close()
	{
	}

	public static ResultSetProvider getDatabaseMetaDataInts()
	throws SQLException
	{
		try
		{
			return new MetaDataInts();
		}
		catch(SQLException e)
		{
			throw new SQLException("Error reading DatabaseMetaData", e
				.getMessage());
		}
	}
}
