/*
 * Copyright (c) 2004, 2005 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Thomas Hallgren
 */
public class Environment
{
	private static final Pattern s_envPattern = Pattern.compile("^(\\w+)=(.*)$");
	private static final boolean s_isWindows;

	private final TreeMap m_env;

	static
	{
		Pattern osPattern  = Pattern.compile("^windows\\W", Pattern.CASE_INSENSITIVE);
		s_isWindows = osPattern.matcher(System.getProperty("os.name")).lookingAt();	
	}

	private static String getEnvCommand()
	{
		return s_isWindows ? "cmd /C set" : "sh -c env";
	}

	public static boolean isWindows()
	{
		return s_isWindows;
	}

	public Environment()
	throws IOException
	{
		String  line;
		TreeMap env = new TreeMap();

		m_env = env;

		Process proc  = Runtime.getRuntime().exec(getEnvCommand());
		BufferedReader lr = new BufferedReader(new InputStreamReader(proc.getInputStream()));
		while((line = lr.readLine()) != null)
		{
			Matcher matcher = s_envPattern.matcher(line);
			if(matcher.matches())
			{
				String key = matcher.group(1);
				String val = matcher.group(2);
				if(s_isWindows)
					env.put(key.toLowerCase(), new String[] { key, val } );
				else
					env.put(key, val);
			}
		}
	}

	public String get(String key)
	{
		if(s_isWindows)
		{
			String[] entry = (String[])m_env.get(key.toLowerCase());
			return (entry == null) ? null : entry[1];
		}
		return (String)m_env.get(key);
	}

	public void put(String key, String val)
	{
		if(s_isWindows)
		{
			String lowKey = key.toLowerCase();
			String[] entry = (String[])m_env.get(lowKey);
			if(entry == null)
				m_env.put(lowKey, new String[] { key, val });
			else
				entry[1] = val;
		}
		else
			m_env.put(key, val);
	}

	public String[] asArray()
	{
		StringBuffer bld = new StringBuffer();
		ArrayList envArr = new ArrayList();
		Iterator itor = m_env.entrySet().iterator();
		while(itor.hasNext())
		{
			Map.Entry entry = (Map.Entry)itor.next();
			if(s_isWindows)
			{
				String[] kv = (String[])entry.getValue();
				bld.append(kv[0]);
				bld.append('=');
				bld.append(kv[1]);
			}
			else
			{
				bld.append(entry.getKey());
				bld.append('=');
				bld.append(entry.getValue());
			}
			envArr.add(bld.toString());
			bld.setLength(0);
		}
		return (String[])envArr.toArray(new String[envArr.size()]);
	}
	
	public String toString()
	{
		StringBuffer bld = new StringBuffer();
		String newLine = System.getProperty("line.separator");
		Iterator itor = m_env.entrySet().iterator();
		while(itor.hasNext())
		{
			Map.Entry entry = (Map.Entry)itor.next();
			if(s_isWindows)
			{
				String[] kv = (String[])entry.getValue();
				bld.append(kv[0]);
				bld.append(" = ");
				bld.append(kv[1]);
				bld.append(newLine);
			}
			else
			{
				bld.append(entry.getKey());
				bld.append(" = ");
				bld.append(entry.getValue());
				bld.append(newLine);
			}
		}
		return bld.toString();
	}
}
