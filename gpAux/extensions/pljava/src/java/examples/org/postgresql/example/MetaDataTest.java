/*
 * Copyright (c) 2004, 2005 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.example;

import java.sql.*;
import java.lang.reflect.*;
import java.util.regex.*;
import java.util.Iterator;
import java.util.ArrayList;

/**
 * @author Filip Hrbek
 */
public class MetaDataTest
{
	public static Iterator callMetaDataMethod(String methodCall) throws SQLException
	{
		return new MetaDataTest(methodCall).iterator();
	}

	private String m_methodName;
	private Object[] m_methodArgs;
	private Class[] m_methodArgTypes;
	private ArrayList m_results;

	public MetaDataTest(String methodCall) throws SQLException
	{
		Connection conn = DriverManager
			.getConnection("jdbc:default:connection");
		DatabaseMetaData md = conn.getMetaData();
        ResultSet rs;
		m_results = new ArrayList();
		StringBuffer result;

		parseMethodCall(methodCall);

		try
		{
			Method m = DatabaseMetaData.class
				.getMethod(m_methodName, m_methodArgTypes);
			if (!m.getReturnType().equals(ResultSet.class))
			{
				throw new NoSuchMethodException("Unexpected return type");
			}

			rs = (ResultSet)m.invoke(md, m_methodArgs);
			ResultSetMetaData rsmd = rs.getMetaData();

			int cnt = rsmd.getColumnCount();
			result = new StringBuffer();
			for (int i=1; i <= cnt; i++)
			{
				result.append(
					(rsmd.getColumnName(i) +
					"(" + rsmd.getColumnClassName(i) + ")"
					)
					.replaceAll("(\\\\|;)","\\$1") + ";");
			}
			m_results.add(result.toString());

			while (rs.next())
			{
				result = new StringBuffer();
				Object rsObject = null;
				for(int i=1; i <= cnt; i++)
				{
                    rsObject = rs.getObject(i);
                    if (rsObject == null)
                    {
                        rsObject = "<NULL>";
                    }

                    result.append(rsObject.toString()
						.replaceAll("(\\\\|;)","\\$1") + ";");
				}
				m_results.add(result.toString());
			}
			rs.close();
		}
		catch (NoSuchMethodException nme)
		{
			StringBuffer sb = new StringBuffer();
			for (int i=0; i < m_methodArgTypes.length; i++)
			{
				if (sb.length() > 0)
				{
					sb.append(",");
				}
				sb.append(m_methodArgTypes[i].getName());
			}
			throw new SQLException("No such method or non-resultset return type: " +
									m_methodName +
								   "(" + sb.toString() + ")");
		}
		catch (InvocationTargetException ite)
		{
			throw new SQLException(ite.getTargetException().toString());
		}
		catch (Exception e)
		{
			throw new SQLException("Method error: " + e.toString());
		}
	}

	private Iterator iterator()
	{
		return m_results.iterator();
	}

	public void close()
	{
	}

	/**
	 * Method call parser.
	 * Let's say that only String, String[], int, int[] and boolean types are accepted.
	 * Examples:
	 *    "foo" => String
	 *    {"foo","bar"} => String[]
	 *    {} => String[] (zero length array, not null!)
	 *    10 => int
	 *    -7 => int
	 *    [10,3] => int[]
	 *    [] => int[] (zero length array, not null!)
	 *    TRUE => boolean
	 *    (String)null => String (value null)
	 *    (String[])null => String[] (value null)
	 *    (int[])null => int[] (value null)
	 *
	 * Complete example:
	 *    select * from callMetaDataMethod('getTables((String)null,"sqlj","jar%",{"TABLE"})');
	 *
	 * It makes the implementation simplier, and it is sufficient for
	 * DatabaseMetaData testing.
	 */
	private void parseMethodCall(String methodCall) throws SQLException
	{
		try
		{
			Pattern p = Pattern.compile("^\\s*([a-zA-Z_][a-zA-Z0-9_]*)\\s*\\((.*)\\)\\s*$");
			Matcher m = p.matcher(methodCall);
			String paramString;
			String auxParamString;
			String param;
			ArrayList objects = new ArrayList();
			ArrayList types = new ArrayList();

			if (m.matches())
			{
				m_methodName = m.group(1);
				paramString = m.group(2).trim();
				p = Pattern.compile(
					"^\\s*(" +
					"\\(\\s*(?:String|int)\\s*(?:\\[\\s*\\])?\\s*\\)\\s*null|" + //String, String[] or int[] null
					"TRUE|" +											 //boolean TRUE
					"FALSE|" +											 //boolean FALSE
					"(?:\\-|\\+)?[0-9]+|" +								 //int
					"\\[((?:[^\\[\\]])*)\\]|" +							 //int[]
					"\"((?:[^\\\\\"]|\\\\.)*)\"|" +						 //String
					"\\{((?:[^\\{\\}]|\"(?:[^\\\\\"]|\\\\.)*\")*)\\}" +	 //String[]
					")\\s*" +
					"(?:,|$)" +											 //comma separator
					"(.*)$");											 //rest of the string
				auxParamString = paramString;
				while(!auxParamString.equals(""))
				{
					m = p.matcher(auxParamString);
					if (!m.matches())
					{
						throw new SQLException("Invalid parameter list: " + paramString);
					}

					param = m.group(1);
					if (param.startsWith("\"")) //it is a string
					{
						param = m.group(3);    //string without the quotes
						objects.add(param);
						types.add(String.class);
					}
					else if (param.startsWith("{")) //it is a string array
					{
						param = m.group(4);    //string array without the curly brackets
						Pattern parr = Pattern.compile("^\\s*\"((?:[^\\\\\"]|\\\\.)*)\"\\s*(?:,|$)(.*)$");
						Matcher marr;
						String auxParamArr = param.trim();
						ArrayList strList = new ArrayList();
						
						while(!auxParamArr.equals(""))
						{
							marr = parr.matcher(auxParamArr);
							if (!marr.matches())
							{
								throw new SQLException("Invalid string array: " + param);
							}

							strList.add(marr.group(1));
							auxParamArr = marr.group(2).trim();
						}
						objects.add(strList.toArray(new String[0]));
						types.add(String[].class);
					}
					else if (param.equals("TRUE") || param.equals("FALSE")) //it is a boolean
					{
						objects.add(new Boolean(param));
						types.add(Boolean.TYPE);
					}
					else if (param.startsWith("(")) //it is String, String[] or int[] null
					{
						Pattern pnull = Pattern.compile("^\\(\\s*(String|int)\\s*(\\[\\s*\\])?\\s*\\)\\s*null\\s*$");
						Matcher mnull = pnull.matcher(param);

						if (mnull.matches())
						{
							objects.add(null);
							if (mnull.group(2) == null)
							{
								if (mnull.group(1).equals("String"))
								{
									types.add(String.class);
								}
								else
								{
									throw new SQLException("Primitive 'int' cannot be null");
								}
							}
							else
							{
								if (mnull.group(1).equals("String"))
								{
									types.add(String[].class);
								}
								else
								{
									types.add(int[].class);
								}
							}
						}
						else
						{
							throw new SQLException("Invalid null value: " + param);
						}
					}
					else if (param.startsWith("[")) //it is a int array
					{
						param = m.group(2);    //int array without the square brackets
						Pattern parr = Pattern.compile("^\\s*(\\d+)\\s*(?:,|$)(.*)$");
						Matcher marr;
						String auxParamArr = param.trim();
						ArrayList intList = new ArrayList();
						
						while(!auxParamArr.equals(""))
						{
							marr = parr.matcher(auxParamArr);
							if (!marr.matches())
							{
								throw new SQLException("Invalid int array: " + param);
							}

							intList.add(new Integer(marr.group(1)));
							auxParamArr = marr.group(2).trim();
						}
						objects.add(intList.toArray(new Integer[0]));
						types.add(int[].class);
					}
					else //it is an int
					{
						objects.add(new Integer(param));
						types.add(Integer.TYPE);
					}

					auxParamString = m.group(5).trim();
				}
			}
			else
			{
				throw new SQLException("Syntax error");
			}

			m_methodArgs = objects.toArray(new Object[0]);
			m_methodArgTypes = (Class[])types.toArray(new Class[0]);
		}
		catch (Exception e)
		{
			throw new SQLException("Invalid method call: " + methodCall + ". Cause: " + e.toString());
		}
	}
}
