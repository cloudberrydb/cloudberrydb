/*
 * Copyright (c) 2004, 2005 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.sqlgen;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;

import org.postgresql.pljava.annotation.Function;

import org.objectweb.asm.ClassReader;

/**
 * @author Thomas Hallgren
 */
public class SQLGenerator
{
	// Test function...
	public static void main(String[] argv)
	{
		try
		{
			StringWriter buf = new StringWriter();
			PrintWriter writer = new PrintWriter(buf);
			for(String arg : argv)
			{
				ClassReader reader = new ClassReader(arg);
				PLJavaClassVisitor pljavaVisitor = new PLJavaClassVisitor();
				reader.accept(pljavaVisitor, true);
				List<FunctionVisitor> functions = pljavaVisitor.getFunctions();
				int top = functions.size();
				for(int idx = 0; idx < top; ++idx)
				{
					FunctionVisitor function = functions.get(idx);
					emittFunction(function, writer);
					writer.println(";");
				}
			}
			writer.flush();

			String stmt = buf.toString();
			System.out.println(stmt);
		}
		catch(Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void emittFunction(FunctionVisitor function, PrintWriter writer)
	{
		writer.print("CREATE OR REPLACE FUNCTION ");
		writer.print(function.getName());
		writer.print('(');
		String[] argTypes = function.getArgumentTypes();
		int top = argTypes.length;
		if(top > 0)
		{
			writer.print(argTypes[0]);
			for(int idx = 1; idx < top; ++idx)
			{
				writer.print(',');
				writer.print(argTypes[idx]);
			}
		}
		writer.println(")");
		writer.print("\tRETURNS ");
		writer.println(function.getReturnType());
		writer.println("\tLANGUAGE java");
		switch(function.getType())
		{
			case STABLE:
				writer.println("\tSTABLE");
				break;
			case IMMUTABLE:
				writer.println("\tIMMUTABLE");
		}
		if(function.getOnNullInput() == Function.OnNullInput.RETURNS_NULL)
			writer.println("\tRETURNS NULL ON NULL INPUT");
		if(function.getSecurity() == Function.Security.DEFINER)
			writer.println("\tSECURITY DEFINER");
			
		writer.print("\tAS '");
		writer.print(function.getClassName());
		writer.print('.');
		writer.print(function.getMethodName());
		writer.print('(');

		String[] paramTypes = function.getParameterTypes();
		top = paramTypes.length;
		if(top > 0)
		{
			writer.print(paramTypes[0]);
			for(int idx = 1; idx < top; ++idx)
			{
				writer.print(',');
				writer.print(paramTypes[idx]);
			}
		}
		writer.print(")'");
	}
}
