/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.management;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.logging.Logger;

import org.postgresql.pljava.Session;
import org.postgresql.pljava.SessionManager;

/**
 * This class deals with parsing and executing the deployment descriptor as
 * defined in ISO/IEC 9075-13:2003. It has the following format:<pre><code>
 * &lt;descriptor file&gt; ::=
 * SQLActions &lt;left bracket&gt; &lt;right bracket&gt; &lt;equal sign&gt;
 * { [ &lt;double quote&gt; &lt;action group&gt; &lt;double quote&gt;
 *   [ &lt;comma&gt; &lt;double quote&gt; &lt;action group&gt; &lt;double quote&gt; ] ] }
 *
 * &lt;action group&gt; ::=
 *     &lt;install actions&gt;
 *   | &lt;remove actions&gt;
 * 
 * &lt;install actions&gt; ::=
 *   BEGIN INSTALL [ &lt;command&gt; &lt;semicolon&gt; ]... END INSTALL
 *
 * &lt;remove actions&gt; ::=
 *   BEGIN REMOVE [ &lt;command&gt; &lt;semicolon&gt; ]... END REMOVE
 *
 * &lt;command&gt; ::=
 *     &lt;SQL statement&gt;
 *   | &lt;implementor block&gt;
 *
 * &lt;SQL statement&gt; ::= &lt;SQL token&gt;...
 * 
 * &lt;implementor block&gt; ::=
 *   BEGIN &lt;implementor name&gt; &lt;SQL token&gt;... END &lt;implementor name&gt;
 *
 * &lt;implementor name&gt; ::= &lt;identifier&gt;
 *
 * &lt;SQL token&gt; ::= an SQL lexical unit specified by the term &quot;&lt;token&gt;&quot; in
 * Subclause 5.2, &quot;&lt;token&gt;&quot; and &quot;&lt;separator&gt;&quot;, in ISO/IEC 9075-2.</code></pre>
 *
 * @author Thomas Hallgren
 */
public class SQLDeploymentDescriptor
{
	private final ArrayList m_installCommands = new ArrayList();
	private final ArrayList m_removeCommands = new ArrayList();
	
	private final StringBuffer m_buffer = new StringBuffer();
	private final char[] m_image;
	private final String m_implementorName;
	private final Logger m_logger;

	private int m_position = 0;

	/**
	 * Parses the deployment descriptor <code>descImage</code> using
	 * <code>implementorName</code> as discriminator for implementor specific
	 * blocks. The install and remove blocks are remembered for later execution
	 * with calls to {@link #install install()} and {@link #remove remove()}.
	 * @param descImage The image to parse
	 * @param implementorName The discriminator to use for implementor blocks
	 * @throws ParseException If a parse error is encountered
	 */
	public SQLDeploymentDescriptor(String descImage, String implementorName)
	throws ParseException
	{
		m_image = descImage.toCharArray();
		m_implementorName = implementorName;
		m_logger = Logger.getAnonymousLogger();
		this.readDescriptor();
	}

	/**
	 * Executes the <code>INSTALL</code> actions.
	 * @param conn The connection to use for the execution.
	 * @throws SQLException
	 */
	public void install(Connection conn)
	throws SQLException
	{
		this.executeArray(m_installCommands, conn);
	}

	/**
	 * Executes the <code>REMOVE</code> actions.
	 * @param conn The connection to use for the execution.
	 * @throws SQLException
	 */
	public void remove(Connection conn)
	throws SQLException
	{
		this.executeArray(m_removeCommands, conn);
	}

	/**
	 * Returns the original image.
	 */
	public String toString()
	{
		return new String(m_image);
	}

	private void executeArray(ArrayList array, Connection conn)
	throws SQLException
	{
		m_logger.entering("org.postgresql.pljava.management.SQLDeploymentDescriptor", "executeArray");
		Session session = SessionManager.current();
		int top = array.size();
		for(int idx = 0; idx < top; ++idx)
		{
			String cmd = (String)array.get(idx);
			m_logger.finer(cmd);
			session.executeAsSessionUser(conn, cmd);
		}
		m_logger.exiting("org.postgresql.pljava.management.SQLDeploymentDescriptor", "executeArray");
	}

	private ParseException parseError(String msg)
	{
		return new ParseException(msg, m_position);
	}

	private void readDescriptor()
	throws ParseException
	{
		m_logger.entering("org.postgresql.pljava.management.SQLDeploymentDescriptor", "readDescriptor");
		if(!"SQLACTIONS".equals(this.readIdentifier()))
			throw this.parseError("Excpected keyword 'SQLActions'");

		this.readToken('[');
		this.readToken(']');
		this.readToken('=');
		this.readToken('{');
		for(;;)
		{
			readActionGroup();
			if(readToken("},") == '}')
			{
				// Only whitespace allowed now
				//
				int c = this.skipWhite();
				if(c >= 0)
					throw this.parseError(
						"Extraneous characters at end of descriptor");
				m_logger.exiting("org.postgresql.pljava.management.SQLDeploymentDescriptor", "readDescriptor");
				return;
			}
		}
	}

	private void readActionGroup()
	throws ParseException
	{
		m_logger.entering("org.postgresql.pljava.management.SQLDeploymentDescriptor", "readActionGroup");
		this.readToken('"');
		if(!"BEGIN".equals(this.readIdentifier()))
			throw this.parseError("Excpected keyword 'BEGIN'");

		ArrayList commands;
		String actionType = this.readIdentifier();
		if("INSTALL".equals(actionType))
			commands = m_installCommands;
		else if("REMOVE".equals(actionType))
			commands = m_removeCommands;
		else
			throw this.parseError("Excpected keyword 'INSTALL' or 'REMOVE'");

		for(;;)
		{
			String cmd = this.readCommand();					

			// Check if the cmd is in the form:
			//
			// <implementor block> ::=
			//	BEGIN <implementor name> <SQL token>... END <implementor name>
			//
			// If it is, and if the implementor name corresponds to the one
			// defined for this deployment, then extract the SQL token stream.
			//
			int top = cmd.length();
			if(top >= 15
			&& "BEGIN ".equalsIgnoreCase(cmd.substring(0, 6))
			&& Character.isJavaIdentifierStart(cmd.charAt(6)))
			{
				int pos;
				for(pos = 7; pos < top; ++pos)
					if(!Character.isJavaIdentifierPart(cmd.charAt(pos)))
						break;

				if(cmd.charAt(pos) != ' ')
					throw this.parseError(
						"Expected whitespace after <implementor name>");

				String implementorName = cmd.substring(6, pos);
				int iLen = implementorName.length();

				int endNamePos = top - iLen;
				int endPos = endNamePos - 4;
				if(!implementorName.equalsIgnoreCase(cmd.substring(endNamePos))
				|| !"END ".equalsIgnoreCase(cmd.substring(endPos, endNamePos)))
					throw this.parseError(
						"Implementor block must end with END <implementor name>");

				if(implementorName.equalsIgnoreCase(m_implementorName))
					cmd = cmd.substring(pos+1, endPos);
				else
					// Block is not intended for this implementor.
					//
					cmd = null;
			}

			if(cmd != null)
				commands.add(cmd.trim());

			// Check if we have END INSTALL or END REMOVE
			//
			int savePos = m_position;
			try
			{
				String tmp = this.readIdentifier();
				if("END".equals(tmp))
				{
					tmp = this.readIdentifier();
					if(actionType.equals(tmp))
						break;
				}
				m_position = savePos;
			}
			catch(ParseException e)
			{
				m_position = savePos;
			}
		}
		this.readToken('"');
		m_logger.exiting("org.postgresql.pljava.management.SQLDeploymentDescriptor", "readActionGroup");
	}

	private String readCommand()
	throws ParseException
	{
		m_logger.entering("org.postgresql.pljava.management.SQLDeploymentDescriptor", "readCommand");
		int startQuotePos = -1;
		int inQuote = 0;
		int c = this.skipWhite();
		m_buffer.setLength(0);
		while(c != -1)
		{
			switch(c)
			{
			case '\\':
				m_buffer.append((char)c);
				c = this.read();
				if(c != -1)
				{
					m_buffer.append((char)c);
					c = this.read();
				}
				break;

			case '"':
			case '\'':
				if(inQuote == 0)
				{
					startQuotePos = m_position;
					inQuote = c;
				}
				else if(inQuote == c)
				{
					startQuotePos = -1;
					inQuote = 0;
				}
				m_buffer.append((char)c);
				c = this.read();
				break;

			case ';':
				if(inQuote == 0)
				{
					String cmd = m_buffer.toString();
					m_logger.exiting("org.postgresql.pljava.management.SQLDeploymentDescriptor", "readCommand", cmd);
					return cmd;
				}

				m_buffer.append((char)c);
				c = this.read();
				break;

			default:
				if(inQuote == 0 && Character.isWhitespace((char)c))
				{
					// Change multiple whitespace into one singe space.
					//
					m_buffer.append(' ');
					c = this.skipWhite();
				}
				else
				{	
					m_buffer.append((char)c);
					c = this.read();
				}
			}
		}
		if(inQuote != 0)
			throw this.parseError("Untermintated " + (char)inQuote +
					" starting at position " + startQuotePos);

		throw this.parseError("Unexpected EOF. Expecting ';' to end command");
	}

	private int skipWhite()
	throws ParseException
	{
		int c;
		for(;;)
		{
			c = this.read();
			if(c >= 0 && Character.isWhitespace((char)c))
				continue;
			
			if(c == '/')
			{
				switch(this.peek())
				{
				// "//" starts a line comment. Skip until end of line.
				//
				case '/':
					this.skip();
					for(;;)
					{
						c = this.read();
						switch(c)
						{
						case '\n':
						case '\r':
						case -1:
							break;
						default:
							continue;
						}
						break;
					}
					continue;

				// "/*" starts a line comment. Skip until "*/"
				//
				case '*':
					this.skip();
					for(;;)
					{
						c = this.read();
						switch(c)
						{
						case -1:
							throw this.parseError(
								"Unexpected EOF when expecting end of multi line comment");
						
						case '*':
							if(this.peek() == '/')
							{
								this.skip();
								break;
							}
							continue;

						default:
							continue;
						}
						break;
					}
					continue;
				}
			}
			break;
		}
		return c;
	}

	private String readIdentifier()
	throws ParseException
	{
		int c = this.skipWhite();
		if(c < 0)
			throw this.parseError("Unexpected EOF when expecting start of identifier");

		char ch = (char)c;
		if(!Character.isJavaIdentifierStart(ch))
			throw this.parseError(
					"Syntax error at '" + ch +
					"', expected identifier");

		m_buffer.setLength(0);
		m_buffer.append(ch);
		for(;;)
		{
			c = this.peek();
			if(c < 0)
				break;

			ch = (char)c;
			if(Character.isJavaIdentifierPart(ch))
			{	
				m_buffer.append(ch);
				this.skip();
				continue;
			}
			break;
		}
		return m_buffer.toString().toUpperCase();
	}

	private char readToken(String tokens)
	throws ParseException
	{
		int c = this.skipWhite();
		if(c < 0)
			throw this.parseError("Unexpected EOF when expecting one of \"" + tokens + '"');

		char ch = (char)c;
		if(tokens.indexOf(ch) < 0)
			throw this.parseError(
				"Syntax error at '" + ch +
				"', expected one of '" + tokens + "'");
		return ch;
	}

	private char readToken(char token)
	throws ParseException
	{
		int c = this.skipWhite();
		if(c < 0)
			throw this.parseError("Unexpected EOF when expecting token '" + token + '\'');

		char ch = (char)c;
		if(ch != token)
			throw this.parseError(
				"Syntax error at '" + ch +
				"', expected '" + token + "'");
		return ch;
	}

	private int peek()
	{
		return (m_position >= m_image.length) ? -1 : m_image[m_position];
	}

	private void skip()
	{
		m_position++;
	}

	private int read()
	{
		int pos = m_position++;
		return (pos >= m_image.length) ? -1 : m_image[pos];
	}
}