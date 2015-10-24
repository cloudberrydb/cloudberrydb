/*
 * Copyright (c) 2004, 2005 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.sqlgen;

import java.util.ArrayList;

import org.objectweb.asm.Type;

/**
 * @author Thomas Hallgren
 */
public class GenericType
{
	private final Type m_type;
	private final GenericType m_genericType;

	public static GenericType forType(String typeDesc, String signature)
	{
		return parseType(typeDesc, 0, new int[1]);
	}

	public final GenericType getGeneric()
	{
		return m_genericType;
	}

	public final Type getType()
	{
		return m_type;
	}

	public static GenericType[] getArgumentTypes(String methodDescriptor)
	{
		final ArrayList<GenericType> args = new ArrayList<GenericType>();
		if(methodDescriptor.charAt(0) == '(')
		{
			int[] endPos = new int[1];
			int top = methodDescriptor.length();
			int pos = 1;
			while(pos < top)
			{
				if(methodDescriptor.charAt(pos) == ')')
					return args.toArray(new GenericType[args.size()]);
				args.add(parseType(methodDescriptor, pos, endPos));
				pos = endPos[0];
			}
		}
		throw new RuntimeException("Malformed method descriptor " + methodDescriptor);
	}

	public static GenericType getReturnType(String methodDescriptor)
	{
		return parseType(
			methodDescriptor.substring(
				methodDescriptor.indexOf(')') + 1), 0, new int[1]);
	}

	public String getClassName()
	{
		if(m_genericType == null)
			return m_type.getClassName();
		
		StringBuilder bld = new StringBuilder();
		this.printClassNameTo(bld);
		return bld.toString();
	}

	public void printClassNameTo(StringBuilder bld)
	{
		bld.append(m_type.getClassName());
		if(m_genericType != null)
		{
			bld.append('<');
			m_genericType.printTo(bld);
			bld.append('>');
		}
	}

	public String toString()
	{
		if(m_genericType == null)
			return m_type.toString();
		
		StringBuilder bld = new StringBuilder();
		this.printTo(bld);
		return bld.toString();
	}

	public void printTo(StringBuilder bld)
	{
		bld.append(m_type.toString());
		if(m_genericType != null)
		{
			bld.setLength(bld.length()-1); // backspace emitted ';'
			bld.append('<');
			m_genericType.printTo(bld);
			bld.append('>');
			bld.append(';');
		}
	}

	private GenericType(Type type)
	{
		m_type = type;
		m_genericType = null;
	}

	private GenericType(Type type, GenericType genericType)
	{
		m_type = type;
		m_genericType = genericType;
	}

	private static GenericType parseType(final String desc, int pos, int[] endPos)
	{
		char c = 0;
		int start = pos;
		int len = desc.length();
		while(pos < len && (c = desc.charAt(pos)) == '[')
			pos++;

		if(c != 'L')
		{
			Type realType = Type.getType(desc.substring(start, ++pos));
			endPos[0] = pos;
			return new GenericType(realType);
		}

		while(++pos < len)
		{
			switch(c = desc.charAt(pos))
			{
				case ';':
					endPos[0] = ++pos;
					return new GenericType(Type.getType(desc.substring(start, pos)));

				case '<':
					Type realType = Type.getType(desc.substring(start, pos) + ';');
					GenericType nested = parseType(desc, ++pos, endPos);
					pos = endPos[0];
					assert(desc.charAt(pos) == '>');
					assert(desc.charAt(pos+1) == ';');
					endPos[0] = pos + 2;
					return new GenericType(realType, nested);
			}
		}
		throw new RuntimeException("Malformed type " + desc);
	}
}
