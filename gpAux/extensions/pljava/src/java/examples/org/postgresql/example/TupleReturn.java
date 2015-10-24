/*
 * Copyright (c) 2004, 2005 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.example;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.postgresql.pljava.ResultSetProvider;

/**
 * @author Thomas Hallgren
 */
public class TupleReturn implements ResultSetProvider
{
	private final int m_base;
	private final int m_increment;
	
	public TupleReturn(int base, int increment)
	{
		m_base = base;
		m_increment = increment;
	}

	public boolean assignRowValues(ResultSet receiver, int currentRow)
	throws SQLException
	{
		// Stop when we reach 12 rows.
		//
		if(currentRow >= 12)
			return false;

		receiver.updateInt(1, m_base);
		receiver.updateInt(2, m_base + m_increment * currentRow);
		receiver.updateTimestamp(3, new Timestamp(System.currentTimeMillis()));
		return true;
	}

	public void close()
	{
	}

	public static ResultSetProvider setReturn(int base, int increment)
	throws SQLException
	{
		return new TupleReturn(base, increment);
	}

	public static boolean tupleReturn(int base, int increment, ResultSet receiver)
	throws SQLException
	{
		receiver.updateInt(1, base);
		receiver.updateInt(2, base + increment);
		receiver.updateTimestamp(3, new Timestamp(System.currentTimeMillis()));
		return true;
	}

	public static boolean tupleReturn(Integer base, Integer increment, ResultSet receiver)
	throws SQLException
	{
		if(base == null)
		{
			receiver.updateNull(1);
			receiver.updateNull(2);
		}
		else
		{
			receiver.updateInt(1, base.intValue());
			if(increment == null)
				receiver.updateNull(2);
			else
				receiver.updateInt(2, base.intValue() + increment.intValue());
		}
		receiver.updateTimestamp(3, new Timestamp(System.currentTimeMillis()));
		return true;
	}
	
	public static String makeString(ResultSet _testSetReturn)
	throws SQLException
	{
		int base = _testSetReturn.getInt(1);
		int incbase = _testSetReturn.getInt(2);
		Timestamp ctime = _testSetReturn.getTimestamp(3);
		return "Base = \"" + base +
			"\", incbase = \"" + incbase +
			"\", ctime = \"" + ctime + "\"";
	}
}
