/*
 * Copyright (c) 2004 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root directory of this distribution or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.example.annotation;

import java.sql.SQLException;

import org.postgresql.pljava.TriggerData;
import org.postgresql.pljava.annotation.Function;
import org.postgresql.pljava.annotation.Trigger;
import static org.postgresql.pljava.annotation.Trigger.When.*;
import static org.postgresql.pljava.annotation.Trigger.Event.*;
import static org.postgresql.pljava.annotation.Function.Security.*;

public class Triggers
{
	/**
	 * insert user name in response to a trigger.
	 */
	@Function(
		schema = "javatest",
		security = INVOKER,
		triggers = {
			@Trigger(when = BEFORE, table = "foobar_1", events = { INSERT } ),
			@Trigger(when = BEFORE, table = "foobar_2", events = { INSERT } )
		})

	public static void insertUsername(TriggerData td)
	throws SQLException
	{
	}
}
