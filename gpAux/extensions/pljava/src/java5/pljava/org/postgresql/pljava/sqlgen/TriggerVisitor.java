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
import org.postgresql.pljava.annotation.Trigger;

/**
 * @author Thomas Hallgren
 */

public class TriggerVisitor extends DefaultAnnotationVisitor
{
	private ArrayList<String> m_arguments;

	private ArrayList<Trigger.Event> m_events;

	private String m_name = "";

	private String m_schema = "";

	private Trigger.Scope m_scope = Trigger.Scope.STATEMENT;

	private String m_table = "";

	private Trigger.When m_when;

	public void visit(String name, Object value)
	{
		if("name".equals(name))
			m_name = (String)value;
		else if("schema".equals(name))
			m_schema = (String)value;
		else if("table".equals(name))
			m_table = (String)value;
		else
			throw new UnrecognizedAttributeException(name);
	}

	public void visitEnum(String name, String desc, String value)
	{
		if("scope".equals(name))
			m_scope = Trigger.Scope.valueOf(value);
		else if("when".equals(name))
			m_when = Trigger.When.valueOf(value);
		else
			throw new UnrecognizedAttributeException(name);
	}

	public AnnotationVisitor visitArray(String name)
	{
		if("arguments".equals(name))
		{
			if(m_arguments == null)
				m_arguments = new ArrayList<String>();
			return new DefaultAnnotationVisitor()
			{
				public void visit(String ignore, Object value)
				{
					m_arguments.add((String)value);
				}
			};
		}

		if("events".equals(name))
		{
			if(m_events == null)
				m_events = new ArrayList<Trigger.Event>();
			return new DefaultAnnotationVisitor()
			{
				public void visitEnum(String ignore, String desc, String value)
				{
					m_events.add(Trigger.Event.valueOf(value));
				}
			};
		}

		throw new UnrecognizedAttributeException(name);
	}

	public void visitEnd()
	{
		if(m_events == null)
			throw new MissingAttributeException("events");
		if(m_table.length() == 0)
			throw new MissingAttributeException("table");
		if(m_when == null)
			throw new MissingAttributeException("when");
		if(m_name.length() == 0)
		{
			StringBuilder bld = new StringBuilder();
			bld.append("trg_");
			bld.append((m_when == Trigger.When.BEFORE) ? 'b' : 'a');
			bld.append((m_scope == Trigger.Scope.ROW) ? 'r' : 's');

			// Fixed order regardless of order in list.
			//
			boolean atDelete = false;
			boolean atInsert = false;
			boolean atUpdate = false;
			int top = m_events.size();
			for(int idx = 0; idx < top; ++idx)
			{
				switch(m_events.get(idx))
				{
					case DELETE:
						atDelete = true;
						break;
					case INSERT:
						atInsert = true;
						break;
					default:
						atUpdate = true;
				}
			}
			bld.append('_');
			if(atDelete)
				bld.append('d');
			if(atInsert)
				bld.append('i');
			if(atUpdate)
				bld.append('u');
			bld.append('_');
			bld.append(m_table);
			m_name = bld.toString();
		}		
	}

	final List<String> getArguments()
	{
		return m_arguments;
	}

	final List<Trigger.Event> getEvents()
	{
		return m_events;
	}

	final String getName()
	{
		return m_name;
	}

	final String getSchema()
	{
		return m_schema;
	}

	final Trigger.Scope getScope()
	{
		return m_scope;
	}

	final String getTable()
	{
		return m_table;
	}

	final Trigger.When getWhen()
	{
		return m_when;
	}
}
