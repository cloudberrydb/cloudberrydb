/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * The SQL 2003 spec. does not stipulate a standard way of mapping
 * triggers to functions. The PLJava mapping use this interface. All
 * functions that are intended to be triggers must be public, static,
 * return void, and take a <code>TriggerData</code> as their argument.
 * 
 * @author Thomas Hallgren
 */
public interface TriggerData
{
	/**
	 * Returns the ResultSet that represents the new row. This ResultSet will
	 * be null for delete triggers and for triggers that was fired for
	 * statement. <br/>The returned set will be updateable and positioned on a
	 * valid row. When the trigger call returns, the trigger manager will see
	 * the changes that has been made to this row and construct a new tuple
	 * which will become the new or updated row.
	 *
	 * @return An updateable <code>ResultSet</code> containing one row or
	 *         <code>null</code>.
	 * @throws SQLException
	 *             if the contained native buffer has gone stale.
	 */
	ResultSet getNew() throws SQLException;

	/**
	 * Returns the ResultSet that represents the old row. This ResultSet will
	 * be null for insert triggers and for triggers that was fired for
	 * statement. <br/>The returned set will be read-only and positioned on a
	 * valid row.
	 * 
	 * @return A read-only <code>ResultSet</code> containing one row or
	 *         <code>null</code>.
	 * @throws SQLException
	 *             if the contained native buffer has gone stale.
	 */
	ResultSet getOld() throws SQLException;


	/**
	 * Returns the arguments for this trigger (as declared in the <code>CREATE TRIGGER</code>
	 * statement. If the trigger has no arguments, this method will return an
	 * array with size 0.
	 * 
	 * @throws SQLException
	 *             if the contained native buffer has gone stale.
	 */
	String[] getArguments() throws SQLException;

	/**
	 * Returns the name of the trigger (as declared in the <code>CREATE TRIGGER</code>
	 * statement).
	 * 
	 * @throws SQLException
	 *             if the contained native buffer has gone stale.
	 */
	String getName() throws SQLException;

	/**
	 * Returns the name of the table for which this trigger was created (as
	 * declared in the <code>CREATE TRIGGER</code statement).
	 * 
	 * @throws SQLException
	 *             if the contained native buffer has gone stale.
	 */
	String getTableName() throws SQLException;

	/**
	 * Returns the name of the schema of the table for which this trigger was created (as
	 * declared in the <code>CREATE TRIGGER</code statement).
	 * 
	 * @throws SQLException
	 *             if the contained native buffer has gone stale.
	 */
	String getSchemaName() throws SQLException;

	/**
	 * Returns <code>true</code> if the trigger was fired after the statement
	 * or row action that it is associated with.
	 * 
	 * @throws SQLException
	 *             if the contained native buffer has gone stale.
	 */
	boolean isFiredAfter() throws SQLException;

	/**
	 * Returns <code>true</code> if the trigger was fired before the
	 * statement or row action that it is associated with.
	 * 
	 * @throws SQLException
	 *             if the contained native buffer has gone stale.
	 */
	boolean isFiredBefore() throws SQLException;

	/**
	 * Returns <code>true</code> if this trigger is fired once for each row
	 * (as opposed to once for the entire statement).
	 * 
	 * @throws SQLException
	 *             if the contained native buffer has gone stale.
	 */
	boolean isFiredForEachRow() throws SQLException;

	/**
	 * Returns <code>true</code> if this trigger is fired once for the entire
	 * statement (as opposed to once for each row).
	 * 
	 * @throws SQLException
	 *             if the contained native buffer has gone stale.
	 */
	boolean isFiredForStatement() throws SQLException;

	/**
	 * Returns <code>true</code> if this trigger was fired by a <code>DELETE</code>.
	 * 
	 * @throws SQLException
	 *             if the contained native buffer has gone stale.
	 */
	boolean isFiredByDelete() throws SQLException;

	/**
	 * Returns <code>true</code> if this trigger was fired by an <code>INSERT</code>.
	 * 
	 * @throws SQLException
	 *             if the contained native buffer has gone stale.
	 */
	boolean isFiredByInsert() throws SQLException;

	/**
	 * Returns <code>true</code> if this trigger was fired by an <code>UPDATE</code>.
	 * 
	 * @throws SQLException
	 *             if the contained native buffer has gone stale.
	 */
	boolean isFiredByUpdate() throws SQLException;
}
