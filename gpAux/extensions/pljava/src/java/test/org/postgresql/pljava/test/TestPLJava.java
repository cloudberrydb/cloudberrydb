/*
 * Copyright (c) 2004, 2005 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.test;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * PL/Java Test harness
 *
 * @author Thomas Hallgren
 */
public class TestPLJava
{
//	private static final int CMD_AMBIGUOUS = -2;
//	private static final int CMD_UNKNOWN   = -1;
	private static final int CMD_PGSQLHOME = 0;
	private static final int CMD_TESTDIR   = 1;
	private static final int CMD_PORT      = 2;

	private static final ArrayList s_commands = new ArrayList();

	private final File m_pgsqlHome;
	private final File m_pljavaHome;
	private final File m_pljavaBin;
	private final File m_testHome;
	private final int m_majorVer;
	private final int m_minorVer;

	static
	{
		s_commands.add(CMD_PGSQLHOME, "pgsqlhome");
		s_commands.add(CMD_TESTDIR,   "testdir");
		s_commands.add(CMD_PORT,      "port");
	}

/*	private static final int getCommand(String arg)
	{
		int top = s_commands.size();
		int candidateCmd = CMD_UNKNOWN;
		for(int idx = 0; idx < top; ++idx)
		{
			if(((String)s_commands.get(idx)).startsWith(arg))
			{
				if(candidateCmd != CMD_UNKNOWN)
					return CMD_AMBIGUOUS;
				candidateCmd = idx;
			}
		}
		return candidateCmd;
	} */
	public static void printUsage()
	{
		PrintStream out = System.err;
		out.println("usage: java org.postgresql.pljava.test.TestPLJava");
		out.println("    [ -port <number>     ]    # default is 5432");
		out.println("    [ -pgsqlhome <dir>   ]    # default is /usr/local/pgsql");
		out.println("    [ -testdir <dir>     ]    # default is current directory");
		out.println("    [ -printenv          ]    # print the env for the postmaster");
		out.println("    [ -windows ]              # If the server is on a Windows machine");
	}

	class KillPostmaster extends Thread
	{
		
	}

	private int[] getPostgreSQLVersion()
	throws IOException
	{
		// Get the PostgreSQL version using pg_ctl
		//
		CommandReader pg_ctl = CommandReader.create(
			new String[] { "pg_ctl", "--version" },
			this.getPostgresEnvironment().asArray());

		String verLine = pg_ctl.readLine();
		pg_ctl.close();
		int exitVal = pg_ctl.getExitValue();
		if(exitVal != 0)
			throw new IOException("pg_ctl exit value " + exitVal);

		Pattern verPattern = Pattern.compile("\\D+(\\d+)\\.(\\d+)\\D");
		Matcher verMatcher = verPattern.matcher(verLine);
		if(!verMatcher.lookingAt())
			throw new IOException("Unable to determine PostgreSQL version from " + verLine);

		return new int[] {
				Integer.parseInt(verMatcher.group(1)), 
				Integer.parseInt(verMatcher.group(2)) };
	}

	public TestPLJava(String pgsqlHome, String pljavaHome)
	throws IOException
	{
		m_pgsqlHome  = new File(pgsqlHome);
		m_pljavaHome = new File(pljavaHome);
		m_testHome   = new File(m_pljavaHome, "test");
		m_pljavaBin  = new File(new File(new File(m_pljavaHome, "bin"), "build"), "pljava");

		int[] ver = this.getPostgreSQLVersion();
		m_majorVer = ver[0];
		m_minorVer = ver[1];
	}

	class Postmaster extends Thread
	{

		Postmaster()
		{
		}

		public void run()
		{
			List args = new ArrayList();
			args.add("postmaster");
			args.add("-D");
			args.add(new File(m_testHome, "db").getAbsolutePath());
			if(m_minorVer < 5)
			{
				args.add("-c");
				args.add("tcpip_socket=true");
			}
			else
			{
				args.add("-c");
				args.add("custom_variable_classes=pljava");
			}
			args.add("-c");
			args.add("log_min_messages=debug1");
			args.add("-c");
			args.add("dynamic_library_path=" + m_pljavaBin.getAbsolutePath());
			
			Runtime rt = Runtime.getRuntime();
			KillPostmaster killer = new KillPostmaster();
			rt.addShutdownHook(killer);
			try
			{
				rt.exec((String[])args.toArray(new String[args.size()]));
			}
			catch(IOException e)
			{
				
			}
			rt.removeShutdownHook(killer);

		}
	}

	public Environment getPostgresEnvironment()
	throws IOException
	{
		Environment env = new Environment();
		Path path = new Path(env.get("PATH"));
		File pgsqlBin = new File(m_pgsqlHome, "bin");
		File pgsqlLib = new File(m_pgsqlHome, "lib");

		if(Environment.isWindows())
			path.addFirst(pgsqlLib);
		else
		{
			Path ldPath = new Path(env.get("LD_LIBRARY_PATH"));
			ldPath.addFirst(pgsqlLib);
			env.put("LD_LIBRARY_PATH", ldPath.toString());
		}

		path.addFirst(pgsqlBin);
		env.put("PATH", path.toString());
		return env;
	}

	public static void main(String[] args)
	{
		try
		{
			TestPLJava tpj = new TestPLJava("c:\\msys\\local\\pgsql", "c:\\");
			System.out.println(tpj.getPostgresEnvironment());
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public void initdb()
	{
		
	}
	public final int getMajorVer()
	{
		return m_majorVer;
	}
	
	public final int getMinorVer()
	{
		return m_minorVer;
	}
	
}
