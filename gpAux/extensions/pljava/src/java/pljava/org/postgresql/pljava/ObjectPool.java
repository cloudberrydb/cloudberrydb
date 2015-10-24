/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root directory of this distribution or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava;

import java.sql.SQLException;

public interface ObjectPool
{
	/**
	 * Obtain a pooled object. A new instance is created if needed. The pooled
	 * object is removed from the pool and activated.
	 * 
	 * @return A new object or an object found in the pool.
	 */
	PooledObject activateInstance()
	throws SQLException;

	/**
	 * Call the {@link PooledObject#passivate()} method and return the object
	 * to the pool.
	 * @param instance The instance to passivate.
	 */
	void passivateInstance(PooledObject instance)
	throws SQLException;

	/**
	 * Call the {@link PooledObject#remove()} method and evict the object
	 * from the pool.
	 */
	void removeInstance(PooledObject instance)
	throws SQLException;
}
