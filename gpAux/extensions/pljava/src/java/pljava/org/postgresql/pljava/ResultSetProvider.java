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
 * An implementation of this interface is returned from functions and procedures
 * that are declared to return <code>SET OF</code> a complex type. Functions that
 * return <code>SET OF</code> a simple type should simply return an
 * {@link java.util.Iterator Iterator}.
 * @author Thomas Hallgren
 */
public interface ResultSetProvider
{
	/**
	 * This method is called once for each row that should be returned from
	 * a procedure that returns a set of rows. The receiver
	 * is a {@link org.postgresql.pljava.jdbc.SingleRowWriter SingleRowWriter}
	 * writer instance that is used for capturing the data for the row.
	 * @param receiver Receiver of values for the given row.
	 * @param currentRow Row number. First call will have row number 0.
	 * @return <code>true</code> if a new row was provided, <code>false</code>
	 * if not (end of data).
	 * @throws SQLException
	 */
	boolean assignRowValues(ResultSet receiver, int currentRow)
	throws SQLException;
	
	/**
	 * Called after the last row has returned or when the query evaluator decides
	 * that it does not need any more rows.
	 */
	void close()
	throws SQLException;
}
