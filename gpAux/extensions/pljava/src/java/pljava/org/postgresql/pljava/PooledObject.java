/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root directory of this distribution or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava;

import java.sql.SQLException;

/**
 * Interface for objects that are pooled and reused.
 * @author Thomas Hallgren
 */
public interface PooledObject
{
	/**
	 * The activate method is called when the instance is activated
	 * from its &quot;passive&quot; state.
	 * @throws SQLException if something goes wrong with the activation.
	 * When this happens, a call will be issued to {@link #remove()} and
	 * the object will no longer be part of the pool.
	 */
	void activate()
	throws SQLException;

	/**
	 * The passivate method is called before the instance enters
	 * the &quot;passive&quot; state.
	 * @throws SQLException if something goes wrong with the passivation.
	 * When this happens, a call will be issued to {@link #remove()} and
	 * the object will no longer be part of the pool.
	 */
	void passivate()
	throws SQLException;

	/**
	 * PLJava invokes this method before it ends the life of the
	 * pooled object.
	 */
	void remove();
}
