/*
 * Copyright (c) 2004, 2005 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.logging.Logger;

import org.postgresql.pljava.TriggerException;
import org.postgresql.pljava.TriggerData;
import org.postgresql.pljava.SessionManager;

/**
 * This class contains some triggers that I found written in C under the
 * contrib/spi directory of the postgres source distribution. Code to create the
 * necessary tables, functions, triggers, and some code to actually
 * execute them can be found in class {@link org.postgresql.pljava.test.Tester}.
 *
 * @author Thomas Hallgren
 */
public class Triggers
{
	/**
	 * insert user name in response to a trigger.
	 */
	public static void insertUsername(TriggerData td)
	throws SQLException
	{
		if(td.isFiredForStatement())
			throw new TriggerException(td, "can't process STATEMENT events");

		if(td.isFiredAfter())
			throw new TriggerException(td, "must be fired before event");

		if(td.isFiredByDelete())
			throw new TriggerException(td, "can't process DELETE events");

		ResultSet _new = td.getNew();
		String[] args = td.getArguments();
		if(args.length != 1)
			throw new TriggerException(td, "one argument was expected");

		if(_new.getString(args[0]) == null)
			_new.updateString(args[0], SessionManager.current().getUserName());
	}

	public static void leakStatements(TriggerData td)
	throws SQLException
	{
		StringBuffer buf = new StringBuffer();
		
		buf.append("Trigger ");
		buf.append(td.getName());
		buf.append(" declared on table ");
		buf.append(td.getTableName());
		buf.append(" was fired ");
		if(td.isFiredAfter())
			buf.append("after");
		else
			buf.append("before");
		
		buf.append(' ');
		if(td.isFiredByDelete())
			buf.append("delete");
		else if(td.isFiredByInsert())
			buf.append("insert");
		else
			buf.append("update");
		
		if(td.isFiredForEachRow())
			buf.append(" on each row");
		
		// DON'T DO LIKE THIS!!! Connection, PreparedStatement, and ResultSet instances
		// should always be closed.
		//
		int max = Integer.MIN_VALUE;
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		PreparedStatement stmt = conn.prepareStatement("SELECT base FROM setReturnExample(?, ?)");
		stmt.setInt(1, 5);
		stmt.setInt(2, 8);
		ResultSet rs = stmt.executeQuery();
		while(rs.next())
		{
			int base = rs.getInt(1);
			if(base > max)
				max = base;
		}
		buf.append(" reports max = " + max);
		stmt = conn.prepareStatement("INSERT INTO javatest.mdt (idesc) VALUES (?)");
		stmt.setString(1, buf.toString());
		stmt.executeUpdate();
	}

	public static void afterUsernameInsert(TriggerData td)
	throws SQLException
	{
		Logger log = Logger.getAnonymousLogger();
		log.info("After username insert, oid of tuple = " + td.getNew().getInt("oid"));
	}

	public static void afterUsernameUpdate(TriggerData td)
	throws SQLException
	{
		Logger log = Logger.getAnonymousLogger();
		if(td.isFiredForStatement())
			throw new TriggerException(td, "can't process STATEMENT events");

		if(td.isFiredBefore())
			throw new TriggerException(td, "must be fired after event");

		if(!td.isFiredByUpdate())
			throw new TriggerException(td, "can't process DELETE or INSERT events");

		ResultSet _new = td.getNew();
		String[] args = td.getArguments();
		if(args.length != 1)
			throw new TriggerException(td, "one argument was expected");
		String colName = args[0];

		ResultSet _old = td.getOld();
		log.info("Old name is \"" + _old.getString(colName) + '"');
		log.info("New name is \"" + _new.getString(colName) + '"');
	}

	/**
	 * Update a modification time when the row is updated.
	 */
	public static void moddatetime(TriggerData td)
	throws SQLException
	{
		if(td.isFiredForStatement())
			throw new TriggerException(td, "can't process STATEMENT events");

		if(td.isFiredAfter())
			throw new TriggerException(td, "must be fired before event");

		if(!td.isFiredByUpdate())
			throw new TriggerException(td, "can only process UPDATE events");

		ResultSet _new = td.getNew();
		String[] args = td.getArguments();
		if(args.length != 1)
			throw new TriggerException(td, "one argument was expected");
		_new.updateTimestamp(args[0], new Timestamp(System.currentTimeMillis()));
	}
}
