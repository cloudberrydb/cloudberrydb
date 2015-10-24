/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * A Session maintains transaction coordinated in-memory data. The data
 * added since the last commit will be lost on a transaction rollback, i.e.
 * the Session state is synchronized with the transaction.
 * 
 * Please note that if nested objects (such as lists and maps) are stored
 * in the session, changes internal to those objects are not subject to
 * the session semantics since the session is unaware of them.
 * 
 * @author Thomas Hallgren
 */
public interface Session
{
	/**
	 * Adds the specified <code>listener</code> to the list of listeners that will
	 * receive savepoint events. This method does nothing if the listener
	 * was already added.
	 * @param listener The listener to be added.
	 */
	void addSavepointListener(SavepointListener listener);

	/**
	 * Adds the specified <code>listener</code> to the list of listeners that will
	 * receive transactional events. This method does nothing if the listener
	 * was already added.
	 * @param listener The listener to be added.
	 */
	void addTransactionListener(TransactionListener listener);

	/**
	 * Obtain an attribute from the current session.
	 * @param attributeName The name of the attribute
	 * @return The value of the attribute
	 */
	Object getAttribute(String attributeName);

	/**
	 * Return an object pool for the given class. The class must implement
	 * the interface {@link PooledObject}.
	 * @param cls
	 * @return An object pool that pools object of the given class.
	 */
	ObjectPool getObjectPool(Class cls);

	/**
	 * Return the name of the effective user. If the currently
	 * executing funciton is declared with <code>SECURITY DEFINER</code>,
	 * then this method returns the name of the user that defined
	 * the function, otherwise, this method will return the same
	 * as {@link #getSessionUserName()}.
	 */
	String getUserName();

	/**
	 * Return the name of the user that owns the current session.
	 */
	String getSessionUserName();

	/**
	 * Execute a statement as a session user rather then the effective
	 * user. This is useful when functions declared using
	 * <code>SECURITY DEFINER</code> wants to give up the definer
	 * rights.
	 * @param conn The connection used for the execution
	 * @param statement The statement to execute
	 * @throws SQLException if something goes wrong when executing.
	 * @see java.sql.Statement#execute(java.lang.String)
	 */
	void executeAsSessionUser(Connection conn, String statement)
	throws SQLException;

	/**
	 * Remove an attribute previously stored in the session. If
	 * no attribute is found, nothing happens.
	 * @param attributeName The name of the attribute.
	 */
	void removeAttribute(String attributeName);

	/**
	 * Removes the specified <code>listener</code> from the list of listeners that will
	 * receive savepoint events. This method does nothing unless the listener is
	 * found.
	 * @param listener The listener to be removed.
	 */
	void removeSavepointListener(SavepointListener listener);

	/**
	 * Removes the specified <code>listener</code> from the list of listeners that will
	 * receive transactional events. This method does nothing unless the listener is
	 * found.
	 * @param listener The listener to be removed.
	 */
	void removeTransactionListener(TransactionListener listener);

	/**
	 * Set an attribute to a value in the current session.
	 * @param attributeName
	 * @param value
	 */
	void setAttribute(String attributeName, Object value);
}
