/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.internal;

import java.sql.SQLException;
import java.util.HashMap;

/**
 * The <code>Oid</code> correspons to the internal PostgreSQL <code>Oid</code>.
 * Should the size of that change from 32 bit, this class must change too.
 * In Java, the InvalidOid is represented as <code>null</code>.
 *
 * @author Thomas Hallgren
 */
public class Oid extends Number
{
	private static final HashMap s_class2typeId = new HashMap();

	private static final HashMap s_typeId2class = new HashMap();
	static
	{
		try
		{
			// Ensure that the SPI JDBC driver is loaded and registered
			// with the java.sql.DriverManager.
			//
			Class.forName("org.postgresql.pljava.jdbc.SPIDriver");
		}
		catch(ClassNotFoundException e)
		{
			throw new ExceptionInInitializerError(e);
		}
	}

	/**
	 * Finds the PostgreSQL well known Oid for the given class.
	 * @param clazz The class.
	 * @return The well known Oid or null if no such Oid could be found.
	 */
	public static Oid forJavaClass(Class clazz)
	{
		return (Oid)s_class2typeId.get(clazz);
	}

	/**
	 * Finds the PostgreSQL well known Oid for a type name.
	 * @param typeString The name of the type, optionally qualified with a namespace.
	 * @return The well known Oid.
	 * @throws SQLException if the type could not be found
	 */
	public static Oid forTypeName(String typeString)
	{
		synchronized(Backend.THREADLOCK)
		{
			return new Oid(_forTypeName(typeString));
		}
	}

	/**
	 * Finds the PostgreSQL well known Oid for the XOPEN Sql type.
	 * @param sqlType The XOPEN type code.
	 * @throws SQLException if the type could not be found
	 */
	public static Oid forSqlType(int sqlType)
	{
		synchronized(Backend.THREADLOCK)
		{
			return new Oid(_forSqlType(sqlType));
		}
	}

	/**
	 * Returns the PostgreSQL type id for the Oid type.
	 */
	public static Oid getTypeId()
	{
		synchronized(Backend.THREADLOCK)
		{
			return _getTypeId();
		}
	}

	/**
	 * A Type well known to PostgreSQL but not known as a standard XOPEN
	 * SQL type can be registered here. This includes types like the Oid
	 * itself and all the geometry related types.
	 * @param clazz The Java class that corresponds to the type id.
	 * @param typeId The well known type id.
	 */
	public static void registerType(Class clazz, Oid typeId)
	{
		s_class2typeId.put(clazz, typeId);
		if(!s_typeId2class.containsKey(typeId))
			s_typeId2class.put(typeId, clazz);
	}

	/*
	 * The native Oid represented as a 32 bit quantity.
	 * See definition in file &quot;include/postgres_ext&quot; of the
	 * PostgreSQL distribution.
	 */
	private final int m_native;

	public Oid(int value)
	{
		m_native = value;
	}

	public double doubleValue()
	{
		return m_native;
	}

	/**
	 * Checks to see if the other object is an <code>Oid</code>, and if so,
	 * if the native value of that <code>Oid</code> equals the native value
	 * of this <code>Oid</code>.
	 * @return true if the objects are equal.
	 */
	public boolean equals(Object o)
	{
		return (o == this) || ((o instanceof Oid) && ((Oid)o).m_native == m_native);
	}

	public float floatValue()
	{
		return m_native;
	}

	public Class getJavaClass()
	throws SQLException
	{
		Class c = (Class)s_typeId2class.get(this);
		if(c == null)
		{
			String className;
			synchronized(Backend.THREADLOCK)
			{
				className = _getJavaClassName(m_native);
			}
			try
			{
				c = Class.forName(getCanonicalClassName(className, 0));
			}
			catch(ClassNotFoundException e)
			{
				throw new SQLException(e.getMessage());
			}
			s_typeId2class.put(this, c);
			s_class2typeId.put(c, this);
		}
		return c;
	}

	/**
	 * The native value is used as the hash code.
	 * @return The hashCode for this <code>Oid</code>.
	 */
	public int hashCode()
	{
		return m_native;
	}

	public int intValue()
	{
		return m_native;
	}

	public long longValue()
	{
		return m_native;
	}

	/**
	 * Returns a string representation of this OID.
	 */
	public String toString()
	{
		return "OID(" + m_native + ')';
	}

	private static String getCanonicalClassName(String name, int nDims)
	{
		if(name.endsWith("[]"))
			return getCanonicalClassName(name.substring(0, name.length() - 2), nDims + 1);

		boolean primitive = true;
		if(name.equals("boolean"))
			name = "Z";
		else if(name.equals("byte"))
			name = "B";
		else if(name.equals("char"))
			name = "C";
		else if(name.equals("double"))
			name = "D";
		else if(name.equals("float"))
			name = "F";
		else if(name.equals("int"))
			name = "I";
		else if(name.equals("long"))
			name = "J";
		else if(name.equals("short"))
			name = "S";
		else
			primitive = false;
		
		if(nDims > 0)
		{
			StringBuffer bld = new StringBuffer();
			while(--nDims >= 0)
				bld.append('[');
			if(primitive)
				bld.append(name);
			else
			{
				bld.append('L');
				bld.append(name);
				bld.append(';');
			}
			name = bld.toString();
		}
		return name;
	}

	private native static int _forTypeName(String typeString);

	private native static int _forSqlType(int sqlType);

	private native static Oid _getTypeId();

	private native static String _getJavaClassName(int nativeOid)
	throws SQLException;
}
