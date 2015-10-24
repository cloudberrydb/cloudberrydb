/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root directory of this distribution or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.example;

import java.io.IOException;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.SQLInput;
import java.sql.SQLOutput;
import java.util.logging.Logger;

public class ComplexScalar implements SQLData
{
	private static Logger s_logger = Logger.getAnonymousLogger();

	private double m_x;
	private double m_y;
	private String m_typeName;

	public static ComplexScalar parse(String input, String typeName) throws SQLException
	{
		try
		{
			StreamTokenizer tz = new StreamTokenizer(new StringReader(input));
			if(tz.nextToken() == '('
			&& tz.nextToken() == StreamTokenizer.TT_NUMBER)
			{
				double x = tz.nval;
				if(tz.nextToken() == ','
				&& tz.nextToken() == StreamTokenizer.TT_NUMBER)
				{
					double y = tz.nval;
					if(tz.nextToken() == ')')
					{
						s_logger.info(typeName + " from string");
						return new ComplexScalar(x, y, typeName);
					}
				}
			}
			throw new SQLException("Unable to parse complex from string \"" + input + '"');
		}
		catch(IOException e)
		{
			throw new SQLException(e.getMessage());
		}
	}

	public ComplexScalar()
	{
	}

	public ComplexScalar(double x, double y, String typeName)
	{
		m_x = x;
		m_y = y;
		m_typeName = typeName;
	}

	public String getSQLTypeName()
	{
		return m_typeName;
	}

	public void readSQL(SQLInput stream, String typeName) throws SQLException
	{
		s_logger.info(typeName + " from SQLInput");
		m_x = stream.readDouble();
		m_y = stream.readDouble();
		m_typeName = typeName;
	}

	public void writeSQL(SQLOutput stream) throws SQLException
	{
		s_logger.info(m_typeName + " to SQLOutput");
		stream.writeDouble(m_x);
		stream.writeDouble(m_y);
	}

	public String toString()
	{
		s_logger.info(m_typeName + " toString");
		StringBuffer sb = new StringBuffer();
		sb.append('(');
		sb.append(m_x);
		sb.append(',');
		sb.append(m_y);
		sb.append(')');
		return sb.toString();
	}

	public static ComplexScalar logAndReturn(ComplexScalar cpl)
	{
		s_logger.info(cpl.getSQLTypeName() + cpl);
		return cpl;
	}
}
