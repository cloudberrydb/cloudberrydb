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
@Documented
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface Function
{
	public enum OnNullInput { CALLED, RETURNS_NULL };

	public enum Security { INVOKER, DEFINER };

	public enum Type { IMMUTABLE, STABLE, VOLATILE };

	String name();
	String schema();

	OnNullInput onNullInput() default OnNullInput.RETURNS_NULL;
	Security security() default Security.INVOKER;
	Type type() default Type.VOLATILE;

	/**
	 * The Triggers that will call this function (if any).
	 */
	Trigger[] triggers();
}
