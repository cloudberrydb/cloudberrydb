/*
 * Copyright (c) 2004 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root directory of this distribution or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.example;

import java.util.logging.Level;
import java.util.logging.Logger;

public class LoggerTest
{
	public static void logMessage(String logLevel, String message)
	{
		Logger.getAnonymousLogger().log(Level.parse(logLevel), message);
	}
}
