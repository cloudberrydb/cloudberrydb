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
public class MalformedTriggerException extends RuntimeException
{
	MalformedTriggerException(FunctionVisitor function)
	{
		super("Function " + function.getClassName() +
			'.' + function.getMethodName() +
			" is not a valid trigger function");
	}
}
