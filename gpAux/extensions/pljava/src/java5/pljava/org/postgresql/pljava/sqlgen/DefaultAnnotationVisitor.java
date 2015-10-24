/*
 * Copyright (c) 2004, 2005 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.sqlgen;

import org.objectweb.asm.AnnotationVisitor;

/**
 * An <code>AnnotationVisitor</code> implementation that does nothing at all.
 * It is intended to be subclassed.
 * @author Thomas Hallgren
 */
public class DefaultAnnotationVisitor implements AnnotationVisitor
{
	public void visit(String name, Object value)
	{
	}

	public void visitEnum(String name, String desc, String value)
	{
	}

	public AnnotationVisitor visitAnnotation(String name, String desc)
	{
		return null;
	}

	public AnnotationVisitor visitArray(String name)
	{
		return null;
	}

	public void visitEnd()
	{
	}
}
