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
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * @author Filip Hrbek
 */
public class ResultSetTest
{
	public static Iterator executeSelect(String selectSQL) throws SQLException
	{
        if (!selectSQL.toUpperCase().trim().startsWith("SELECT "))
        {
            throw new SQLException("Not a SELECT statement");
        }

		return new ResultSetTest(selectSQL).iterator();
	}

	private ArrayList m_results;

	public ResultSetTest(String selectSQL) throws SQLException
	{
		Connection conn = DriverManager
			.getConnection("jdbc:default:connection");
		m_results = new ArrayList();
		StringBuffer result;

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(selectSQL);
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

	private Iterator iterator()
	{
		return m_results.iterator();
	}

	public void close()
	{
	}
}
