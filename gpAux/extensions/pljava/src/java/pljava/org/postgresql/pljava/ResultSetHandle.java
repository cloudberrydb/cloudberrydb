/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root directory of this distribution or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * An implementation of this interface is returned from functions and procedures
 * that are declared to return <code>SET OF</code> a complex type in the form
 * of a {@link java.sql.ResultSet}. The primary motivation for this interface is
 * that an implementation that returns a ResultSet must be able to close the
 * connection and statement when no more rows are requested.
 * @author Thomas Hallgren
 */
public interface ResultSetHandle
{
	/**
	 * An implementation of this method will probably execute a query
	 * and return the result of that query.
	 * @return The ResultSet that represents the rows to be returned.
	 * @throws SQLException
	 */
	ResultSet getResultSet()
	throws SQLException;

	/**
	 * Called after the last row has returned or when the query evaluator decides
	 * that it does not need any more rows.
	 */
	void close()
	throws SQLException;
}
