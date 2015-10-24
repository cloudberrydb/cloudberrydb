/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.jdbc;

import java.sql.SQLException;

/**
 * @author <a href="mailto:thomas.hallgren@ironjug.com">Thomas Hallgren</a>
 */
public class UnsupportedFeatureException extends SQLException
{
	private static final long serialVersionUID = 7956037664745636982L;

	public static final String FEATURE_NOT_SUPPORTED_EXCEPTION = "0A000";

	public UnsupportedFeatureException(String feature)
	{
		super("Feature not supported: " + feature, FEATURE_NOT_SUPPORTED_EXCEPTION);
	}
}
