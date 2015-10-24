/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.jdbc;

import java.sql.ParameterMetaData;
import java.sql.SQLException;

/**
 *
 * @author Thomas Hallgren
 */
public class SPIParameterMetaData implements ParameterMetaData
{
	private final int[] m_sqlTypes;
	
	SPIParameterMetaData(int[] sqlTypes)
	{
		m_sqlTypes = sqlTypes;
	}

	public int getParameterCount()
	throws SQLException
	{
		return m_sqlTypes == null ? 0 : m_sqlTypes.length;
	}

	public int isNullable(int arg0)
	throws SQLException
	{
		return parameterNullableUnknown;
	}

	public boolean isSigned(int arg0)
	throws SQLException
	{
		return true;
	}

	public int getPrecision(int arg0)
	throws SQLException
	{
		return 0;
	}

	public int getScale(int arg0)
	throws SQLException
	{
		return 0;
	}

	public int getParameterType(int paramIndex)
	throws SQLException
	{
		if(paramIndex < 1 || paramIndex > this.getParameterCount())
			throw new SQLException("Parameter index out of range");
		return m_sqlTypes[paramIndex-1];
	}

	/**
	 * This feature is not yet supported.
	 * @throws SQLException indicating that this feature is not supported.
	 */
	public String getParameterTypeName(int arg0)
	throws SQLException
	{
		throw new UnsupportedFeatureException("Parameter type name support not yet implemented");
	}

	/**
	 * This feature is not yet supported.
	 * @throws SQLException indicating that this feature is not supported.
	 */
	public String getParameterClassName(int arg0)
	throws SQLException
	{
		throw new UnsupportedFeatureException("Parameter class name support not yet implemented");
	}

	/**
	 * Returns {@link ParameterMetaData#parameterModeIn} always since this
	 * is the only supported type at this time.
	 */
	public int getParameterMode(int paramIndex) throws SQLException
	{
		if(paramIndex < 1 || paramIndex > this.getParameterCount())
			throw new SQLException("Parameter index out of range");
		return parameterModeIn;
	}

	/**
	 * Returns true if this either implements the interface argument or is
	 * directly or indirectly a wrapper for an object that does. Returns false
	 * otherwise. If this implements the interface then return true, else if
	 * this is a wrapper then return the result of recursively calling
	 * isWrapperFor on the wrapped object. If this does not implement the
	 * interface and is not a wrapper, return false. This method should be
	 * implemented as a low-cost operation compared to unwrap so that callers
	 * can use this method to avoid expensive unwrap calls that may fail. If
	 * this method returns true then calling unwrap with the same argument
	 * should succeed.
	 *
	 * @since 1.6
	 */
	public boolean isWrapperFor(Class iface)
	throws SQLException
	{
		throw new UnsupportedFeatureException("isWrapperFor");
	}

	/**
	 * Returns an object that implements the given interface to allow access to
	 * non-standard methods, or standard methods not exposed by the proxy. If
	 * the receiver implements the interface then the result is the receiver or
	 * a proxy for the receiver. If the receiver is a wrapper and the wrapped
	 * object implements the interface then the result is the wrapped object or
	 * a proxy for the wrapped object. Otherwise return the the result of
	 * calling unwrap recursively on the wrapped object or a proxy for that
	 * result. If the receiver is not a wrapper and does not implement the
	 * interface, then an SQLException is thrown.
	 *
	 * @since 1.6
	 */
	public Object unwrap(Class iface)
	throws SQLException
	{
		throw new UnsupportedFeatureException("unwrap");
	}
}