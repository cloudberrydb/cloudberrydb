/*
 * Copyright (c) 2004 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root directory of this distribution or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.example;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

public class Security
{
	/**
	 * The following method should fail if the language in use
	 * is untrusted.
	 * @return The name of a created temporary file.
	 * @throws SQLException
	 */
	public static String createTempFile()
	throws SQLException
	{
		try
		{
			File tmp = File.createTempFile("pljava", ".test");
			tmp.deleteOnExit();
			return tmp.getAbsolutePath();
		}
		catch(IOException e)
		{
			throw new SQLException(e.getMessage());
		}
	}
}
