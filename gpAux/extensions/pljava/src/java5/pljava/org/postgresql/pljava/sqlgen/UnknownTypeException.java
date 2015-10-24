/*
 * Copyright (c) 2004, 2005 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.sqlgen;

/**
 * @author Thomas Hallgren
 */
public class UnknownTypeException extends RuntimeException
{
	UnknownTypeException(GenericType type)
	{
		super("Don't know how to map " + type.getClassName() + " to a SQL type");
	}
}
