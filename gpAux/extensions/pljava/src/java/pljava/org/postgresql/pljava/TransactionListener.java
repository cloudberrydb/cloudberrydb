/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava;

import java.sql.SQLException;

/**
 * @author Thomas Hallgren
 */
public interface TransactionListener
{
	void onAbort(Session session) throws SQLException;

	void onCommit(Session session) throws SQLException;

	void onPrepare(Session session) throws SQLException;
}
