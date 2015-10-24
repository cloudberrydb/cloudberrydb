/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.deploy;

import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.ArrayList;

/**
 * When running the deployer, you must use a classpath that can see the
 * deploy.jar found in the Pl/Java distribution and the postgresql.jar from the
 * PostgreSQL distribution. The former contains the code for the deployer
 * command and the second includes the PostgreSQL JDBC driver. You then run the
 * deployer with the command:
 * </p>
 * <blockquote><code>
 * java -cp &lt;your classpath&gt; org.postgresql.pljava.deploy.Deployer [ options ]
 * </code></blockquote>
 * <p>
 * It's recommended that create a shell script or a .bat script that does this
 * for you so that you don't have to do this over and over again.
 * </p>
 * <h3>Deployer options</h3>
 * <blockquote><table>
 * <th>Option</th>
 * <th>Description</th>
 * <tr>
 * <td valign="top">-install</td>
 * <td>Installs the Java language along with the sqlj procedures. The deployer
 * will fail if the language is installed already.</td>
 * </tr>
 * <tr>
 * <td valign="top">-reinstall</td>
 * <td>Reinstalls the Java language and the sqlj procedures. This will
 * effectively drop all jar files that have been loaded.</td>
 * </tr>
 * <tr>
 * <td valign="top">-remove</td>
 * <td>Drops the Java language and the sqjl procedures and loaded jars</td>
 * </tr>
 * <tr>
 * <td nowrap="true" valign="top">-user &lt;user name&gt;</td>
 * <td>Name of user that connects to the database. Default is current user</td>
 * </tr>
 * <tr>
 * <td nowrap="true" valign="top">-password &lt;password&gt;</td>
 * <td>Password of user that connects to the database. Default is no password
 * </td>
 * </tr>
 * <tr>
 * <td nowrap="true" valign="top">-database &lt;database&gt;</td>
 * <td>The name of the database to connect to. Default is current user</td>
 * </tr>
 * <tr>
 * <td nowrap="true" valign="top">-host &lt;hostname&gt;</td>
 * <td>Name of the host. Default is "localhost"</td>
 * </tr>
 * <tr>
 * <td valign="top">-windows</td>
 * <td>Use this option if the host runs on a windows platform. Affects the
 * name used for the Pl/Java dynamic library</td>
 * </tr>
 * </table></blockquote>
 * 
 * @author Thomas Hallgren
 */
public class Deployer
{
	private static final int CMD_AMBIGUOUS = -2;
	private static final int CMD_UNKNOWN   = -1;
	private static final int CMD_UNINSTALL = 0;
	private static final int CMD_INSTALL   = 1;
	private static final int CMD_REINSTALL = 2;
	private static final int CMD_USER      = 3;
	private static final int CMD_PASSWORD  = 4;
	private static final int CMD_DATABASE  = 5;
	private static final int CMD_HOSTNAME  = 6;
	private static final int CMD_PORT      = 7;
	
	private final Connection m_connection;

	private static final ArrayList s_commands = new ArrayList();
	
	static
	{
		s_commands.add(CMD_UNINSTALL, "uninstall");
		s_commands.add(CMD_INSTALL,   "install");
		s_commands.add(CMD_REINSTALL, "reinstall");
		s_commands.add(CMD_USER,      "user");
		s_commands.add(CMD_PASSWORD,  "password");
		s_commands.add(CMD_DATABASE,  "database");
		s_commands.add(CMD_HOSTNAME,  "host");
		s_commands.add(CMD_PORT,      "port");
	}

	private static final int getCommand(String arg)
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
	}
	public static void printUsage()
	{
		PrintStream out = System.err;
		out.println("usage: java org.postgresql.pljava.deploy.Deployer");
		out.println("    {-install | -uninstall | -reinstall}");
		out.println("    [ -host <hostName>     ]    # default is localhost");
		out.println("    [ -port <portNumber>   ]    # default is blank");
		out.println("    [ -database <database> ]    # default is name of current user");
		out.println("    [ -user <userName>     ]    # default is name of current user");
		out.println("    [ -password <password> ]    # default is no password");
	}

	public static void main(String[] argv)
	{
		String driverClass = "org.postgresql.Driver";
		String hostName    = "localhost";
		String userName    = System.getProperty("user.name", "postgres");
		String database    = userName;
		String subsystem   = "postgresql";
		String password    = null;
		String portNumber  = null;
		int cmd = CMD_UNKNOWN;

		int top = argv.length;
		for(int idx = 0; idx < top; ++idx)
		{
			String arg = argv[idx];
			if(arg.length() < 2)
			{	
				printUsage();
				return;
			}

			if(arg.charAt(0) == '-')
			{
				int optCmd = getCommand(arg.substring(1));
				switch(optCmd)
				{
				case CMD_INSTALL:
				case CMD_UNINSTALL:
				case CMD_REINSTALL:
					if(cmd != CMD_UNKNOWN)
					{	
						printUsage();
						return;
					}
					cmd = optCmd;
					break;

				case CMD_USER:
					if(++idx < top)
					{
						userName = argv[idx];
						if(userName.length() > 0
						&& userName.charAt(0) != '-')
							break;
					}
					printUsage();
					return;

				case CMD_PASSWORD:
					if(++idx < top)
					{
						password = argv[idx];
						if(password.length() > 0
						&& password.charAt(0) != '-')
							break;
					}
					printUsage();
					return;

				case CMD_DATABASE:
					if(++idx < top)
					{
						database = argv[idx];
						if(database.length() > 0
						&& database.charAt(0) != '-')
							break;
					}
					printUsage();
					return;

				case CMD_HOSTNAME:
					if(++idx < top)
					{
						hostName = argv[idx];
						if(hostName.length() > 0
						&& hostName.charAt(0) != '-')
							break;
					}
					printUsage();
					return;

				case CMD_PORT:
					if(++idx < top)
					{
						portNumber = argv[idx];
						if(portNumber.length() > 0
						&& portNumber.charAt(0) != '-')
							break;
					}
					printUsage();
					return;

				default:
					printUsage();
					return;
				}
			}
		}
		if(cmd == CMD_UNKNOWN)
		{	
			printUsage();
			return;
		}

		try
		{
			Class.forName(driverClass);
			
			StringBuffer cc = new StringBuffer();
			cc.append("jdbc:");
			cc.append(subsystem);
			cc.append("://");
			cc.append(hostName);
			if(portNumber != null)
			{	
				cc.append(':');
				cc.append(portNumber);
			}
			cc.append('/');
			cc.append(database);
			Connection c = DriverManager.getConnection(
					cc.toString(),
					userName,
					password);

			checkIfConnectedAsSuperuser(c);
			c.setAutoCommit(false);
			Deployer deployer = new Deployer(c);

			if(cmd == CMD_UNINSTALL || cmd == CMD_REINSTALL)
			{
				deployer.dropSQLJSchema();
			}

			if(cmd == CMD_INSTALL || cmd == CMD_REINSTALL)
			{
				deployer.createSQLJSchema();
				deployer.initJavaHandlers();
				deployer.initializeSQLJSchema();
			}
			c.commit();
			c.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}

	public Deployer(Connection c)
	{
		m_connection = c;
	}

	public static void checkIfConnectedAsSuperuser(Connection conn)
	throws SQLException
	{
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("SHOW IS_SUPERUSER");

		try
		{
			if(rs.next() && rs.getString(1).equals("on"))
				return;
		}
		finally
		{
			rs.close();
			stmt.close();
		}

		throw new SQLException(
			"You must be a superuser to deploy/undeploy pl/Java.");
	}

	public void dropSQLJSchema()
	throws SQLException
	{
		Statement stmt = m_connection.createStatement();
		Savepoint p = null;

		try
		{
			if (m_connection.getMetaData().supportsSavepoints())
				p = m_connection.setSavepoint();
			stmt.execute("DROP LANGUAGE java CASCADE");
			stmt.execute("DROP LANGUAGE javaU CASCADE");
		}
		catch(SQLException e)
		{
			/* roll back to savepoint (if available)
			 * or restart the transaction (if no savepoint is available)
			 * & ignore the exception */
			
			if (p != null)
				m_connection.rollback(p);
			else
				/* Assuming that the dropSQLJSchema is the
				 * first method called in a transaction,
				 * we can afford to restart the transaction.
				 * 
				 * This solution is designed for PostgreSQL < 8 (no savepoints available)
				 */
				m_connection.rollback();
		}
		finally
		{
			if (p != null)
				m_connection.releaseSavepoint(p);
		}

		stmt.execute("DROP SCHEMA sqlj CASCADE");
		stmt.close();
	}

	public void createSQLJSchema()
	throws SQLException
	{
		Statement stmt = m_connection.createStatement();
		stmt.execute("CREATE SCHEMA sqlj");
		stmt.execute("GRANT USAGE ON SCHEMA sqlj TO public");
		stmt.close();
	}

	public void initializeSQLJSchema()
	throws SQLException
	{
		Statement stmt = m_connection.createStatement();

		stmt.execute(
			"CREATE TABLE sqlj.jar_repository(" +
			"	jarId		SERIAL PRIMARY KEY," +
			"	jarName		VARCHAR(100) UNIQUE NOT NULL," +
			"	jarOrigin   VARCHAR(500) NOT NULL," +
			"	jarOwner	NAME NOT NULL," +
			"	jarManifest	TEXT," +
			"   deploymentDesc INT" +
		")");
		stmt.execute("GRANT SELECT ON sqlj.jar_repository TO public");

		stmt.execute(
			"CREATE TABLE sqlj.jar_entry(" +
			"   entryId     SERIAL PRIMARY KEY," +
			"	entryName	VARCHAR(200) NOT NULL," +
			"	jarId		INT NOT NULL REFERENCES sqlj.jar_repository ON DELETE CASCADE," +
			"   entryImage  BYTEA NOT NULL," +
			"   UNIQUE(jarId, entryName)" +
			")");

		stmt.execute("GRANT SELECT ON sqlj.jar_entry TO public");

		stmt.execute(
			"ALTER TABLE sqlj.jar_repository" +
			"   ADD FOREIGN KEY (deploymentDesc) REFERENCES sqlj.jar_entry ON DELETE SET NULL");

		// Create the table maintaining the class path.
		//
		stmt.execute(
			"CREATE TABLE sqlj.classpath_entry(" +
			"	schemaName	VARCHAR(30) NOT NULL," +
			"	ordinal		INT2 NOT NULL," +	// Ordinal in class path
			"	jarId		INT NOT NULL REFERENCES sqlj.jar_repository ON DELETE CASCADE," +
			"	PRIMARY KEY(schemaName, ordinal)" +
			")");
		stmt.execute("GRANT SELECT ON sqlj.classpath_entry TO public");

		// Create the table maintaining the SQL to Java type mappings
		//
		stmt.execute(
			"CREATE TABLE sqlj.typemap_entry(" +
			"	mapId		SERIAL PRIMARY KEY," +
			"	javaName	VARCHAR(200) NOT NULL," +
			"	sqlName		NAME NOT NULL" +
			")");
		stmt.execute("GRANT SELECT ON sqlj.typemap_entry TO public");

		// These are the proposed SQL standard methods.
		//
		stmt.execute(
			"CREATE FUNCTION sqlj.install_jar(VARCHAR, VARCHAR, BOOLEAN) RETURNS void" +
			"	AS 'org.postgresql.pljava.management.Commands.installJar'" +
			"	LANGUAGE java SECURITY DEFINER");

		stmt.execute(
			"CREATE FUNCTION sqlj.replace_jar(VARCHAR, VARCHAR, BOOLEAN) RETURNS void" +
			"	AS 'org.postgresql.pljava.management.Commands.replaceJar'" +
			"	LANGUAGE java SECURITY DEFINER");

		stmt.execute(
			"CREATE FUNCTION sqlj.remove_jar(VARCHAR, BOOLEAN) RETURNS void" +
			"	AS 'org.postgresql.pljava.management.Commands.removeJar'" +
			"	LANGUAGE java SECURITY DEFINER");

		// Not proposed, but very useful if you want to send the image over
		// your JDBC connection.
		//
		stmt.execute(
			"CREATE FUNCTION sqlj.install_jar(BYTEA, VARCHAR, BOOLEAN) RETURNS void" +
			"	AS 'org.postgresql.pljava.management.Commands.installJar'" +
			"	LANGUAGE java SECURITY DEFINER");

		stmt.execute(
			"CREATE FUNCTION sqlj.replace_jar(BYTEA, VARCHAR, BOOLEAN) RETURNS void" +
			"	AS 'org.postgresql.pljava.management.Commands.replaceJar'" +
			"	LANGUAGE java SECURITY DEFINER");

		// This function is not as proposed. It's more Java'ish. The proposal
		// using sqlj.alter_jar_path is in my opinion bloated and will not be
		// well received in the Java community. Luckily, the support is suggested
		// to be optional.
		//
		stmt.execute(
			"CREATE FUNCTION sqlj.set_classpath(VARCHAR, VARCHAR) RETURNS void" +
			"	AS 'org.postgresql.pljava.management.Commands.setClassPath'" +
			"	LANGUAGE java SECURITY DEFINER");

		stmt.execute(
			"CREATE FUNCTION sqlj.get_classpath(VARCHAR) RETURNS VARCHAR" +
			"	AS 'org.postgresql.pljava.management.Commands.getClassPath'" +
			"	LANGUAGE java STABLE SECURITY DEFINER");

		// The following functions are not included in the standard. Type mapping
		// is radically different in SQL 2003 and requires a lot of additions to
		// the PostgreSQL dialect.
		//
		stmt.execute(
			"CREATE FUNCTION sqlj.add_type_mapping(VARCHAR, VARCHAR) RETURNS void" +
			"	AS 'org.postgresql.pljava.management.Commands.addTypeMapping'" +
			"	LANGUAGE java SECURITY DEFINER");

		stmt.execute(
			"CREATE FUNCTION sqlj.drop_type_mapping(VARCHAR) RETURNS void" +
			"	AS 'org.postgresql.pljava.management.Commands.dropTypeMapping'" +
			"	LANGUAGE java SECURITY DEFINER");

		stmt.close();
	}

	public void initJavaHandlers()
	throws SQLException
	{
		Statement stmt = m_connection.createStatement();
		stmt.execute(
			"CREATE FUNCTION sqlj.java_call_handler()" +
			" RETURNS language_handler" +
			" AS 'pljava'" +
			" LANGUAGE C");

		stmt.execute("CREATE TRUSTED LANGUAGE java HANDLER sqlj.java_call_handler");

		stmt.execute(
			"CREATE FUNCTION sqlj.javau_call_handler()" +
			" RETURNS language_handler" +
			" AS 'pljava'" +
			" LANGUAGE C");

		stmt.execute("CREATE LANGUAGE javaU HANDLER sqlj.javau_call_handler");
		stmt.close();
	}
}
