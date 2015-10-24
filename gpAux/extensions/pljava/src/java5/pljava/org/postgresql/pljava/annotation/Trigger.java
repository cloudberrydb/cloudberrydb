/*
 * Copyright (c) 2004, 2005 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.annotation;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author Thomas Hallgren
 */
@Target({}) @Retention(RetentionPolicy.CLASS)
public @interface Trigger
{
	enum When { BEFORE, AFTER };

	enum Event { DELETE, INSERT, UPDATE };

	enum Scope { STATEMENT, ROW };

	/**
	 * Arguments to be passed to the trigger function.
	 */
	String[] arguments() default {};

	/**
	 * The event(s) that will trigger the call.
	 */
	Event[] events();

	/**
	 * Name of the trigger. If not set, the name will
	 * be generated.
	 */
	String name() default "";

	/**
	 * The name of the schema where containing the table for
	 * this trigger.
	 */
	String schema() default "";

	/**
	 * The table that this trigger is tied to.
	 */
	String table();

	/**
	 * Scope, i.e. statement or row.
	 */
	Scope scope() default Scope.STATEMENT;
	
	/**
	 * Denotes if the trigger is fired before or after its
	 * scope (row or statement)
	 */
	When when();
}
