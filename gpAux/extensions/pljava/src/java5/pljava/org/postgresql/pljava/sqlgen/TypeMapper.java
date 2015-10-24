/*
 * Copyright (c) 2004, 2005 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.sqlgen;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Iterator;

import org.objectweb.asm.Type;

/**
 * @author Thomas Hallgren
 */

public class TypeMapper
{
	private static final TypeMapper s_singleton = new TypeMapper();

	private final HashMap<String, String> m_typeMap = new HashMap<String, String>();

	private final Type ITERATOR = Type.getType(Iterator.class);

	public static TypeMapper getDefault()
	{
		return s_singleton;
	}

	public String getSQLType(GenericType type)
	{
		String sqlType = m_typeMap.get(type.getType().getDescriptor());
		if(sqlType != null)
			return sqlType;

		// The type descriptor might contain significant
		// generic info.
		//
		GenericType gType = type.getGeneric();
		if(gType == null)
			throw new UnknownTypeException(type);

		if(type.getType().equals(ITERATOR))
			return "SET OF " + this.getSQLType(gType);

		throw new UnknownTypeException(type);
	}

	private TypeMapper()
	{
		// Primitives
		//
		this.addMap(boolean.class, "boolean");
		this.addMap(Boolean.class, "boolean");
		this.addMap(byte.class, "smallint");
		this.addMap(Byte.class, "smallint");
		this.addMap(char.class, "smallint");
		this.addMap(Character.class, "smallint");
		this.addMap(double.class, "double precision");
		this.addMap(Double.class, "double precision");
		this.addMap(float.class, "real");
		this.addMap(Float.class, "real");
		this.addMap(int.class, "integer");
		this.addMap(Integer.class, "integer");
		this.addMap(long.class, "bigint");
		this.addMap(Long.class, "bigint");
		this.addMap(short.class, "smallint");
		this.addMap(Short.class, "smallint");

		// Known common mappings
		//
		this.addMap(String.class, "varchar");
		this.addMap(java.util.Date.class, "timestamp");
		this.addMap(Timestamp.class, "timestamp");
		this.addMap(Time.class, "time");
		this.addMap(java.sql.Date.class, "date");
		this.addMap(BigInteger.class, "numeric");
		this.addMap(BigDecimal.class, "numeric");

		this.addMap(byte[].class, "bytea");
	}

	private void addMap(Class c, String sqlType)
	{
		m_typeMap.put(Type.getDescriptor(c), sqlType);
	}
}
