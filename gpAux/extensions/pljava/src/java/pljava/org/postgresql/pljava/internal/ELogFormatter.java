/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.internal;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.MessageFormat;
import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

/**
 * A default formatter for the ELogHandler.
 *
 * @author Thomas Hallgren
 */
public class ELogFormatter extends Formatter
{
	private final static MessageFormat s_tsFormatter = new MessageFormat(
			"{0,date,dd MMM yy} {0,time,HH:mm:ss} {1} {2}");

	private final static String s_lineSeparator = System.getProperty("line.separator");

	private final Date m_timestamp = new Date();
	private final Object m_args[] = new Object[] { m_timestamp, null, null };
	private final StringBuffer m_buffer = new StringBuffer();

	/**
	 * Format the given LogRecord.
	 * @param record the log record to be formatted.
	 * @return a formatted log record
	 */
	public synchronized String format(LogRecord record)
	{
		StringBuffer sb = m_buffer;
		sb.setLength(0);

		m_timestamp.setTime(record.getMillis());
		String tmp = record.getSourceClassName();
		m_args[1] = (tmp == null) ? record.getLoggerName() : tmp;
		m_args[2] = this.formatMessage(record);
		s_tsFormatter.format(m_args, sb, null);

		Throwable thrown = record.getThrown();
		if(thrown != null)
		{
			sb.append(s_lineSeparator);
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			record.getThrown().printStackTrace(pw);
			pw.close();
			sb.append(sw.toString());
		}
		return sb.toString();
	}
}
