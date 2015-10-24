/*
 * Copyright (c) 2004, 2005 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.example;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class contains thread related methods.
 *
 * @author Thomas Hallgren
 */
public class Threads
{
	private final static String s_lockOne = "lock number one";
	private final static String s_lockTwo = "lock number two";

	static class Locker extends Thread
	{
		private final String m_name;
		private final String m_first;
		private final String m_second;

		Locker(String name, String first, String second)
		{
			m_name  = name;
			m_first = first;
			m_second = second;
		}

		public void run()
		{
			Logger log = Logger.getAnonymousLogger();
			try
			{
				log.info("Thread " + m_name + " wants " + m_first);
				synchronized(m_first)
				{
					log.info("Thread " + m_name + " got " + m_first);
					Thread.sleep(100);
					log.info("Thread " + m_name + " wants " + m_second);
					synchronized(m_second)
					{
						log.info("Thread " + m_name + " got " + m_second);
					}
				}
			}
			catch(Exception e)
			{
				log.log(Level.INFO, "Thread " + m_name + " got exception: ", e);
			}
		}
	}

	public static void forceDeadlock()
	{
		// Cause a deadlock.
		//
		Locker x = new Locker("x", s_lockOne, s_lockTwo);
		Locker y = new Locker("y", s_lockTwo, s_lockOne);
		x.start();
		y.start();

		// Sleep for a while. The logger will fail if trying to access the
		// backend when it is not in a Java call.
		//
		try
		{
			Logger.getAnonymousLogger().log(Level.INFO, "Main thread sleeps a while");
			Thread.sleep(1000);
		}
		catch(Exception e)
		{
			Logger.getAnonymousLogger().log(Level.INFO, "Main thread interrupted while sleeping: ", e);
		}
	}
}
