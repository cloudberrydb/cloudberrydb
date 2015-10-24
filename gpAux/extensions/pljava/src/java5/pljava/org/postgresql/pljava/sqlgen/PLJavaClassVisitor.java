/*
 * Copyright (c) 2004, 2005 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.sqlgen;

import java.util.ArrayList;
import java.util.List;

import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Type;
import static org.objectweb.asm.Opcodes.*;

import org.postgresql.pljava.annotation.Function;

/**
 * @author Thomas Hallgren
 */
public class PLJavaClassVisitor extends DefaultClassVisitor
{
	private final String FUNCTION = Type.getDescriptor(Function.class);

	private final ArrayList<FunctionVisitor> m_functions = new ArrayList<FunctionVisitor>();

	private String m_className;

	class MethodHandler extends DefaultMethodVisitor
	{
		private final String m_name;

		private final String m_signature;

		private final String m_descriptor;

		MethodHandler(String name, String descriptor, String signature)
		{
			m_name = name;
			m_signature = signature;
			m_descriptor = descriptor;
		}

		public AnnotationVisitor visitAnnotation(String desc, boolean visible)
		{
			if(FUNCTION.equals(desc))
			{
				FunctionVisitor function = new FunctionVisitor(
					m_className, m_name, m_descriptor, m_signature);
				m_functions.add(function);
				return function;
			}
			
			// Other annotations may exist but we don't care about them.
			//
			return null;
		}
	}

	public void visit(int version, int access, String name, String signature,
		String superName, String[] interfaces)
	{
		m_className = name.replace('/', '.');
	}

	public FieldVisitor visitField(int access, String name, String desc,
		String signature, Object value)
	{
		return null;
	}
	public MethodVisitor visitMethod(int access, String name, String desc,
		String signature, String[] exceptions)
	{
		// Where' only interested in methods declared as public and static.
		//
		return ((access & ACC_STATIC) != 0 && (access & ACC_PUBLIC) != 0)
			? new MethodHandler(name, desc, signature)
			: null;
	}

	public final List<FunctionVisitor> getFunctions()
	{
		return m_functions;
	}
}
