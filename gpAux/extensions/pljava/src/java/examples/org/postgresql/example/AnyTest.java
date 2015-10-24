/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.example;

import java.lang.reflect.Array;
import java.sql.SQLException;
import java.util.logging.Logger;

public class AnyTest
{
	private static Logger s_logger = Logger.getAnonymousLogger();

	public static void logAny(Object param)
	throws SQLException
	{
		s_logger.info("logAny received an object of class " + param.getClass());
	}

	public static Object logAnyElement(Object param)
	throws SQLException
	{
		s_logger.info("logAnyElement received an object of class " + param.getClass());
		return param;
	}

	public static Object[] makeArray(Object param)
	{
		Object[] result = (Object[])Array.newInstance(param.getClass(), 1);
		result[0] = param;
		return result;
	}
}
