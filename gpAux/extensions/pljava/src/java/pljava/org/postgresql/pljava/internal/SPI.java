/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.internal;

/**
 * The <code>SPI</code> class provides access to some global
 * variables used by SPI.
 *
 * @author Thomas Hallgren
 */
public class SPI
{
	public static final int ERROR_CONNECT		= -1;
	public static final int ERROR_COPY			= -2;
	public static final int ERROR_OPUNKNOWN		= -3;
	public static final int ERROR_UNCONNECTED	= -4;
	public static final int ERROR_CURSOR		= -5;
	public static final int ERROR_ARGUMENT		= -6;
	public static final int ERROR_PARAM			= -7;
	public static final int ERROR_TRANSACTION	= -8;
	public static final int ERROR_NOATTRIBUTE	= -9;
	public static final int ERROR_NOOUTFUNC		= -10;
	public static final int ERROR_TYPUNKNOWN	= -11;

	public static final int OK_CONNECT			= 1;
	public static final int OK_FINISH			= 2;
	public static final int OK_FETCH			= 3;
	public static final int OK_UTILITY			= 4;
	public static final int OK_SELECT			= 5;
	public static final int OK_SELINTO			= 6;
	public static final int OK_INSERT			= 7;
	public static final int OK_DELETE			= 8;
	public static final int OK_UPDATE			= 9;
	public static final int OK_CURSOR			= 10;

	/**
	 * Execute a command using the internal <code>SPI_exec</code> function.
	 * @param command The command to execute.
	 * @param rowCount The maximum number of tuples to create. A value
	 * of <code>rowCount</code> of zero is interpreted as no limit, i.e.,
	 * run to completion.
	 * @return One of the declared status codes.
	 */
	public static int exec(String command, int rowCount)
	{
		synchronized(Backend.THREADLOCK)
		{
			return _exec(System.identityHashCode(Thread.currentThread()), command, rowCount);
		}
	}

	public static void freeTupTable()
	{
		synchronized(Backend.THREADLOCK)
		{
			_freeTupTable();
		}
	}

	/**
	 * Returns the value of the global variable <code>SPI_processed</code>.
	 */
	public static int getProcessed()
	{
		synchronized(Backend.THREADLOCK)
		{
			return _getProcessed();
		}
	}

	/**
	 * Returns the value of the global variable <code>SPI_result</code>.
	 */
	public static int getResult()
	{
		synchronized(Backend.THREADLOCK)
		{
			return _getResult();
		}
	}

	/**
	 * Returns the value of the global variable <code>SPI_tuptable</code>.
	 */
	public static TupleTable getTupTable(TupleDesc known)
	{
		synchronized(Backend.THREADLOCK)
		{
			return _getTupTable(known);
		}
	}

	/**
	 * Returns a textual representatio of a result code
	 */
	public static String getResultText(int resultCode)
	{
		String s;
		switch(resultCode)
		{
			case ERROR_CONNECT:
				s = "ERROR_CONNECT";
				break;
			case ERROR_COPY:
				s = "ERROR_COPY";
				break;
			case ERROR_OPUNKNOWN:
				s = "ERROR_OPUNKNOWN";
				break;
			case ERROR_UNCONNECTED:
				s = "ERROR_UNCONNECTED";
				break;
			case ERROR_CURSOR:
				s = "ERROR_CURSOR";
				break;
			case ERROR_ARGUMENT:
				s = "ERROR_ARGUMENT";
				break;
			case ERROR_PARAM:
				s = "ERROR_PARAM";
				break;
			case ERROR_TRANSACTION:
				s = "ERROR_TRANSACTION";
				break;
			case ERROR_NOATTRIBUTE:
				s = "ERROR_NOATTRIBUTE";
				break;
			case ERROR_NOOUTFUNC:
				s = "ERROR_NOOUTFUNC";
				break;
			case ERROR_TYPUNKNOWN:
				s = "ERROR_TYPUNKNOWN";
				break;
			case OK_CONNECT:
				s = "OK_CONNECT";
				break;
			case OK_FINISH:
				s = "OK_FINISH";
				break;
			case OK_FETCH:
				s = "OK_FETCH";
				break;
			case OK_UTILITY:
				s = "OK_UTILITY";
				break;
			case OK_SELECT:
				s = "OK_SELECT";
				break;
			case OK_SELINTO:
				s = "OK_SELINTO";
				break;
			case OK_INSERT:
				s = "OK_INSERT";
				break;
			case OK_DELETE:
				s = "OK_DELETE";
				break;
			case OK_UPDATE:
				s = "OK_UPDATE";
				break;
			case OK_CURSOR:
				s = "OK_CURSOR";
				break;
			default:
				s = "Unkown result code: " + resultCode;
		}
	return s;
	}

	private native static int _exec(long threadId, String command, int rowCount);
	private native static int _getProcessed();
	private native static int _getResult();
	private native static void _freeTupTable();
	private native static TupleTable _getTupTable(TupleDesc known);
}
