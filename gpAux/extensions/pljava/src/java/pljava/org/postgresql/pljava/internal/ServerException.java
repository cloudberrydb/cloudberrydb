/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.internal;

import java.sql.SQLException;

/**
 * @author Thomas Hallgren
 */
public class ServerException extends SQLException
{
	private static final long serialVersionUID = 8812755938793744633L;

	private transient final ErrorData m_errorData;

	public ServerException(ErrorData errorData)
	{
		super(errorData.getMessage(), errorData.getSqlState());
		m_errorData = errorData;
	}

	public final ErrorData getErrorData()
	{
		return m_errorData;
	}
}
