/*
 * Copyright (c) 2004, 2005 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.test;

import java.io.File;
import java.util.ArrayList;

/**
 * PL/Java Test harness
 *
 * @author Thomas Hallgren
 */
public class Path
{
	public final ArrayList m_path;

	public Path(String path)
	{
		m_path = new ArrayList();

		if(path != null)
		{
			char pathSep = File.pathSeparatorChar;
			int  sep = path.indexOf(pathSep);
			while(sep >= 0)
			{
				if(sep > 0)
					this.addLast(new File(path.substring(0, sep)));
				path = path.substring(sep + 1);
				sep  = path.indexOf(pathSep);
			}
			if(path.length() > 0)
				this.addLast(new File(path));
		}
	}

	public void addFirst(File dir)
	{
		int pos = m_path.indexOf(dir);
		if(pos >= 0)
		{
			if(pos == 0)
				return;
			m_path.remove(pos);
		}
		m_path.add(0, dir);
	}

	public void addLast(File dir)
	{
		int pos = m_path.indexOf(dir);
		if(pos >= 0)
		{
			if(pos == m_path.size() - 1)
				return;
			m_path.remove(pos);
		}
		m_path.add(dir);
	}
	
	public String toString()
	{
		int top = m_path.size();
		if(top == 0)
			return "";

		String first = m_path.get(0).toString();
		if(top == 1)
			return first;

		StringBuffer bld = new StringBuffer();
		char pathSep = File.pathSeparatorChar;
		bld.append(first);
		for(int idx = 1; idx < top; ++idx)
		{
			bld.append(pathSep);
			bld.append(m_path.get(idx));
		}
		return bld.toString();
	}
}
