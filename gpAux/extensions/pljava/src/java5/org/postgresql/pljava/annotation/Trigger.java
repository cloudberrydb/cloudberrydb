/*
 * Copyright (c) 2004 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root directory of this distribution or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author Thomas Hallgren
 */
@Target({}) @Retention(RetentionPolicy.RUNTIME)
public @interface Trigger
{
	enum When { BEFORE, AFTER };

	enum Event { DELETE, INSERT, UPDATE };

	enum Scope { STATEMENT, ROW };

	String[] arguments();
	Event[] events();
	String name();
	String schema();
	Scope scope() default Scope.STATEMENT;
	String table();
	When when();
}
