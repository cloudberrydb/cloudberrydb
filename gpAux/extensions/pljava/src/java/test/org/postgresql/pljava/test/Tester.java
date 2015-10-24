/*
 * Copyright (c) 2004, 2005 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.test;

import java.sql.Timestamp;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Pattern;
import java.io.PrintStream;

/**
 * Some fairly crude tests. All tests are confided to the schema
 * &quot;javatest&quot;
 * 
 * @author Thomas Hallgren
 */
public class Tester
{
	private static final int CMD_AMBIGUOUS = -2;

	private static final int CMD_UNKNOWN = -1;

	private static final int CMD_USER = 0;

	private static final int CMD_PASSWORD = 1;

	private static final int CMD_DATABASE = 2;

	private static final int CMD_HOSTNAME = 3;

	private static final int CMD_PORT = 4;

	private static final int CMD_DEBUG = 5;

	private static final int CMD_EXCLUDE = 6;

	private final Connection m_connection;

	private static final ArrayList s_commands = new ArrayList();

	static
	{
		s_commands.add(CMD_USER, "user");
		s_commands.add(CMD_PASSWORD, "password");
		s_commands.add(CMD_DATABASE, "database");
		s_commands.add(CMD_HOSTNAME, "host");
		s_commands.add(CMD_PORT, "port");
		s_commands.add(CMD_DEBUG, "debug");
		s_commands.add(CMD_EXCLUDE, "exclude");
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
		out.println("usage: java org.postgresql.pljava.test.Tester");
		out.println("    [ -host <hostName>     ]    # default is localhost");
		out.println("    [ -port <portNubmer>   ]    # default is blank");
		out
			.println("    [ -database <database> ]    # default is name of current user");
		out
			.println("    [ -user <userName>     ]    # default is name of current user");
		out.println("    [ -password <password> ]    # default is no password");
		out
			.println("    [ -exclude excludePattern ] # exclude tests matching pattern");
		out
			.println("    [ -debug ]    # wait for debugger to attach to backend");
	}

	public static void main(String[] argv)
	{
		String driverClass = "org.postgresql.Driver";
		String hostName = "localhost";
		String portNumber = null;
		String userName = System.getProperty("user.name", "postgres");
		String database = userName;
		String subsystem = "postgresql";
		String password = null;
		String exclude = null;
		boolean debug = false;

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
					case CMD_DEBUG:
						debug = true;
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

					case CMD_EXCLUDE:
						if(++idx < top)
						{
							exclude = argv[idx];
							if(exclude.length() > 0 && exclude.charAt(0) != '-')
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
			Connection c = DriverManager.getConnection(cc.toString(), userName,
				password);

			Tester t = new Tester(c);
			if(debug)
			{
				System.out.println("Attach debugger to backend");
				Thread.sleep(30000);
				System.out.println("continuing");
			}
			for(int idx = 0; idx < 10; ++idx)
			{
				Pattern p = (exclude == null) ? null : Pattern.compile(exclude, Pattern.CASE_INSENSITIVE);

				if(p == null || !p.matcher("parameters").matches())
					t.testParameters();
				if(p == null || !p.matcher("usernametrigger").matches())
					t.testInsertUsernameTrigger();
				if(p == null || !p.matcher("moddatetimetrigger").matches())
					t.testModdatetimeTrigger();
				if(p == null || !p.matcher("spiactions").matches())
					t.testSPIActions();
				if(p == null || !p.matcher("tuplereturn").matches())
					t.testTupleReturn();
				if(p == null || !p.matcher("setreturn").matches())
					t.testSetReturn();
				if(p == null || !p.matcher("callincall").matches())
					t.testCallInCall();
				if(p == null || !p.matcher("currentdir").matches())
					t.testCurrentDir();
				if(p == null || !p.matcher("usingproperties").matches())
					t.testUsingProperties();
				if(p == null || !p.matcher("usingscalarproperties").matches())
					t.testUsingScalarProperties();
				if(p == null || !p.matcher("usingresultsetproperties").matches())
					t.testUsingResultSetProperties();
				if(p == null || !p.matcher("savepointsanity").matches())
					t.testSavepointSanity();
				if(p == null || !p.matcher("transactionrecovery").matches())
					t.testTransactionRecovery();
				if(p == null || !p.matcher("trustedsecurity").matches())
					t.testTrustedSecurity();
				if(p == null || !p.matcher("binarycolumns").matches())
					t.testBinaryColumns();
				if(p == null || !p.matcher("databasemetadata").matches())
					t.testDatabaseMetaData();
				if(p == null || !p.matcher("resultset").matches())
					t.testResultSet();
				if(p == null || !p.matcher("complexscalar").matches())
					t.testComplexScalar();
				if(p == null || !p.matcher("complextuple").matches())
					t.testComplexTuple();
			}
			t.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}

	public Tester(Connection conn) throws SQLException
	{
		Statement stmt = conn.createStatement();
		stmt.execute("SET search_path TO javatest,public");
		stmt.close();
		m_connection = conn;
	}

	public void close() throws SQLException
	{
		m_connection.close();
	}

	public void testParameters() throws SQLException
	{
		this.testTimestamp();
		this.testInt();
	}

	public void testSPIActions() throws SQLException
	{
		System.out.println("*** testSPIActions()");
		Statement stmt = m_connection.createStatement();

		stmt.execute("DELETE FROM employees1");
		stmt.execute("DELETE FROM employees2");
		stmt.execute("INSERT INTO employees1 VALUES("
			+ "1, 'Calvin Forrester', 10000)");
		stmt.execute("INSERT INTO employees1 VALUES("
			+ "2, 'Edwin Archer', 20000)");
		stmt.execute("INSERT INTO employees1 VALUES("
			+ "3, 'Rebecka Shawn', 30000)");
		stmt.execute("INSERT INTO employees1 VALUES("
			+ "4, 'Priscilla Johnson', 25000)");

		stmt.execute("SELECT transferPeople(20000)");

		ResultSet rs = stmt.executeQuery("SELECT * FROM employees2");
		while(rs.next())
		{
			int id = rs.getInt(1);
			String name = rs.getString(2);
			int salary = rs.getInt(3);
			System.out.println("Id = \"" + id + "\", name = \"" + name
				+ "\", salary = \"" + salary + "\"");
		}
		rs.close();
	}

	public void testTupleReturn() throws SQLException
	{
		System.out.println("*** testTupleReturn()");
		Statement stmt = m_connection.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT tupleReturnToString(tupleReturnExample(1, 5))");
		while(rs.next())
		{
			String str = rs.getString(1);
			System.out.println(str);
		}
		rs.close();

		rs = stmt.executeQuery("SELECT tupleReturnToString(tupleReturnExample2(1, NULL))");
		while(rs.next())
		{
			String str = rs.getString(1);
			System.out.println(str);
		}
		rs.close();
	}

	public void testSetReturn() throws SQLException
	{
		System.out.println("*** testSetReturn()");
		Statement stmt = m_connection.createStatement();
		ResultSet rs = stmt
			.executeQuery("SELECT base, incbase, ctime FROM setReturnExample(1, 5)");
		while(rs.next())
		{
			int base = rs.getInt(1);
			int incbase = rs.getInt(2);
			Timestamp ctime = rs.getTimestamp(3);
			System.out.println("Base = \"" + base + "\", incbase = \""
				+ incbase + "\", ctime = \"" + ctime + "\"");
		}
		rs.close();
	}

	public void testUsingProperties() throws SQLException
	{
		System.out.println("*** testUsingProperties()");
		Statement stmt = m_connection.createStatement();
		ResultSet rs = stmt
			.executeQuery("SELECT name, value FROM propertyExample()");
		while(rs.next())
		{
			String name = rs.getString(1);
			String value = rs.getString(2);
			System.out.println("Name = \"" + name + "\", value = \"" + value
				+ "\"");
		}
		rs.close();
	}

	public void testUsingResultSetProperties() throws SQLException
	{
		System.out.println("*** testUsingResultSetProperties()");
		Statement stmt = m_connection.createStatement();
		ResultSet rs = stmt
			.executeQuery("SELECT name, value FROM resultSetPropertyExample()");
		while(rs.next())
		{
			String name = rs.getString(1);
			String value = rs.getString(2);
			System.out.println("Name = \"" + name + "\", value = \"" + value
				+ "\"");
		}
		rs.close();
	}

	public void testUsingScalarProperties() throws SQLException
	{
		System.out.println("*** testUsingScalarProperties()");
		Statement stmt = m_connection.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT scalarPropertyExample()");
		while(rs.next())
			System.out.println(rs.getString(1));
		rs.close();
	}

	public void testBinaryColumns() throws SQLException
	{
		System.out.println("*** testBinaryColumns()");

		Statement stmt = m_connection.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT * FROM binaryColumnTest()");
		while(rs.next())
		{
			byte[] b1 = rs.getBytes(1);
			byte[] b2 = rs.getBytes(2);
			if(!Arrays.equals(b1, b2))
				throw new SQLException("binary columns differ");
		}
		rs.close();
	}

	public void testCallInCall() throws SQLException
	{
		System.out.println("*** testCallInCall()");
		Statement stmt = m_connection.createStatement();
		ResultSet rs = stmt
			.executeQuery("SELECT maxFromSetReturnExample(10, 8)");
		while(rs.next())
		{
			int max = rs.getInt(1);
			System.out.println("Max = \"" + max + "\"");
		}
	}

	public void testModdatetimeTrigger() throws SQLException
	{
		System.out.println("*** testModdatetimeTrigger()");
		Statement stmt = m_connection.createStatement();
		stmt.execute("DELETE FROM mdt");
		stmt.execute("INSERT INTO mdt VALUES (1, 'first')");
		stmt.execute("INSERT INTO mdt VALUES (2, 'second')");
		stmt.execute("INSERT INTO mdt VALUES (3, 'third')");

		ResultSet rs = stmt.executeQuery("SELECT * FROM mdt");
		while(rs.next())
		{
			int id = rs.getInt(1);
			String idesc = rs.getString(2);
			Timestamp moddate = rs.getTimestamp(3);
			System.out.println("Id = \"" + id + "\", idesc = \"" + idesc
				+ "\", moddate = \"" + moddate + "\"");
		}
		rs.close();

		stmt.execute("UPDATE mdt SET id = 4 WHERE id = 1");
		stmt.execute("UPDATE mdt SET id = 5 WHERE id = 2");
		stmt.execute("UPDATE mdt SET id = 6 WHERE id = 3");

		rs = stmt.executeQuery("SELECT * FROM mdt");
		while(rs.next())
		{
			int id = rs.getInt(1);
			String idesc = rs.getString(2);
			Timestamp moddate = rs.getTimestamp(3);
			System.out.println("Id = \"" + id + "\", idesc = \"" + idesc
				+ "\", moddate = \"" + moddate + "\"");
		}
		rs.close();
		stmt.close();
	}

	public void testInsertUsernameTrigger() throws SQLException
	{
		System.out.println("*** testInsertUsernameTrigger()");
		Statement stmt = m_connection.createStatement();
		stmt.execute("DELETE FROM username_test");
		stmt.execute("INSERT INTO username_test VALUES ('nothing', 'thomas')");
		stmt.execute("INSERT INTO username_test VALUES ('null', null)");
		stmt.execute("INSERT INTO username_test VALUES ('empty string', '')");
		stmt.execute("INSERT INTO username_test VALUES ('space', ' ')");
		stmt.execute("INSERT INTO username_test VALUES ('tab', '	')");
		stmt.execute("INSERT INTO username_test VALUES ('name', 'name')");

		ResultSet rs = stmt.executeQuery("SELECT * FROM username_test");
		while(rs.next())
		{
			String name = rs.getString(1);
			String username = rs.getString(2);
			System.out.println("Name = \"" + name + "\", username = \""
				+ username + "\"");
		}
		rs.close();

		// Test the update trigger as well as leaking statements
		//
		// System.out.println("Doing 800 updates with double triggers that leak
		// statements");
		// System.out.println("(but not leak memory). One trigger executes SQL
		// that reenters PL/Java");
		// for(int idx = 0; idx < 200; ++idx)
		// {
		// stmt.execute("UPDATE username_test SET username = 'Kalle Kula' WHERE
		// username = 'name'");
		// stmt.execute("UPDATE username_test SET username = 'Pelle Kanin' WHERE
		// username = 'thomas'");
		// stmt.execute("UPDATE username_test SET username = 'thomas' WHERE
		// username = 'Kalle Kula'");
		// stmt.execute("UPDATE username_test SET username = 'name' WHERE
		// username = 'Pelle Kanin'");
		// }
		stmt.close();
	}

	public void testTimestamp() throws SQLException
	{
		System.out.println("*** testTimestamp()");
		Statement stmt = m_connection.createStatement();
		ResultSet rs = stmt
			.executeQuery("SELECT java_getTimestamp(), java_getTimestamptz()");

		if(!rs.next())
			System.out.println("Unable to position ResultSet");
		else
			System.out.println("Timestamp = " + rs.getTimestamp(1)
				+ ", Timestamptz = " + rs.getTimestamp(2));
		rs.close();

		// Test parameter overloading. Set log_min_messages (in posgresql.conf)
		// to INFO or higher and watch the result.
		//
		stmt.execute("SELECT print(current_date)");
		stmt.execute("SELECT print(current_time)");
		stmt.execute("SELECT print(current_timestamp)");
		stmt.close();
	}

	public void testInt() throws SQLException
	{
		System.out.println("*** testInt()");
		/*
		 * Test parameter override from int primitive to java.lang.Integer Test
		 * return value override (stipulated by the Java method rather than in
		 * the function declaration. Seems to be that way according to the
		 * SQL-2003 spec). Test function call within function call and function
		 * that returns an object rather than a primitive to reflect null
		 * values.
		 */
		Statement stmt = m_connection.createStatement();
		ResultSet rs = stmt
			.executeQuery("SELECT java_addOne(java_addOne(54)), nullOnEven(1), nullOnEven(2)");
		if(!rs.next())
			System.out.println("Unable to position ResultSet");
		else
		{
			System.out.println("54 + 2 = " + rs.getInt(1));
			int n = rs.getInt(2);
			System.out.println("nullOnEven(1) = "
				+ (rs.wasNull() ? "null" : Integer.toString(n)));
			n = rs.getInt(3);
			System.out.println("nullOnEven(2) = "
				+ (rs.wasNull() ? "null" : Integer.toString(n)));
		}
		rs.close();
		stmt.close();
	}

	public void testComplexScalar() throws SQLException
	{
		System.out.println("*** testComplexScalar()");
		Statement stmt = m_connection.createStatement();
		ResultSet rs = stmt
			.executeQuery("SELECT logcomplex('(34.56,12.78)'::complex)");
		if(!rs.next())
			System.out.println("Unable to position ResultSet");
		else
			System.out.println(rs.getString(1));
		rs.close();
		stmt.close();
	}

	public void testComplexTuple() throws SQLException
	{
		System.out.println("*** testComplexTuple()");
		Statement stmt = m_connection.createStatement();
		ResultSet rs = stmt
			.executeQuery("SELECT logcomplex((34.56,12.78)::complextuple)");
		if(!rs.next())
			System.out.println("Unable to position ResultSet");
		else
			System.out.println(rs.getString(1));
		rs.close();
		stmt.close();
	}

	public void testCurrentDir() throws SQLException
	{
		System.out.println("*** testCurrentDir()");
		Statement stmt = m_connection.createStatement();
		ResultSet rs = stmt
			.executeQuery("SELECT java_getSystemProperty('user.dir')");

		if(!rs.next())
			System.out.println("Unable to position ResultSet");
		else
			System.out.println("Server directory = " + rs.getString(1));
		rs.close();
		stmt.close();
	}

	public void testSavepointSanity() throws SQLException
	{
		System.out.println("*** testSavepointSanity()");
		Statement stmt = m_connection.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT testSavepointSanity()");

		if(!rs.next())
			System.out.println("Unable to position ResultSet");
		else
			System.out.println("Savepoint sanity = " + rs.getInt(1));
		rs.close();
		stmt.close();
	}

	public void testTransactionRecovery() throws SQLException
	{
		System.out.println("*** testTransactionRecovery()");
		Statement stmt = m_connection.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT testTransactionRecovery()");

		if(!rs.next())
			System.out.println("Unable to position ResultSet");
		else
			System.out.println("Transaction recovery = " + rs.getInt(1));
		rs.close();
		stmt.close();
	}

	public void testTrustedSecurity() throws SQLException
	{
		System.out.println("*** testTrustedSecurity()");
		Statement stmt = m_connection.createStatement();
		ResultSet rs = null;

		tryCreateTempFile(true);

		boolean funcCreated = false;
		try
		{
			rs = stmt.executeQuery("SHOW IS_SUPERUSER");
			if(!(rs.next() && rs.getString(1).equals("on")))
			{
				System.out
					.println("Tester is not superuser so tests on untrusted language cannot be performed");
				return;
			}

			// Try the same java method again, this time using language javaU
			//
			System.out.println("*** testUntrustedSecurity()");
			stmt
				.execute("CREATE OR REPLACE FUNCTION javatest.create_temp_file_untrusted()"
					+ "  RETURNS varchar"
					+ "  AS 'org.postgresql.pljava.example.Security.createTempFile'"
					+ "  LANGUAGE javaU");

			tryCreateTempFile(false);
		}
		finally
		{
			if(rs != null)
			{
				try
				{
					rs.close();
				}
				catch(SQLException e)
				{
				}
				rs = null;
			}
			if(funcCreated)
				stmt
					.execute("DROP FUNCTION javatest.create_temp_file_untrusted()");
			stmt.close();
		}
	}

	private void tryCreateTempFile(boolean trusted) throws SQLException
	{
		Statement stmt = m_connection.createStatement();
		ResultSet rs = null;

		try
		{
			if(trusted)
				rs = stmt.executeQuery("SELECT create_temp_file_trusted()");
			else
				rs = stmt.executeQuery("SELECT create_temp_file_untrusted()");

			if(!rs.next())
				System.out.println("Unable to position ResultSet");
			else
				System.out.println("Name of created temp file = "
					+ rs.getString(1));

			if(trusted)
				throw new RuntimeException(
					"ERROR: Tempfile creation succeded although language is trusted!");
		}
		catch(SQLException e)
		{
			if(!trusted)
				throw e;
			System.out
				.println("OK, creation of temp file was *unsuccessful* as it should be");
		}
		finally
		{
			if(rs != null)
			{
				try
				{
					rs.close();
				}
				catch(SQLException e)
				{
				}
				rs = null;
			}
			stmt.close();
		}
	}

	public void testDatabaseMetaData() throws SQLException
	{
		Statement stmt = m_connection.createStatement();
		ResultSet rs = null;

		try
		{
			System.out.println("*** DatabaseMetaData 'String' functions:");
			rs = stmt
				.executeQuery("SELECT * FROM javatest.getMetaDataStrings()");
			while(rs.next())
			{
				String methodName = rs.getString(1);
				String methodResult = rs.getString(2);
				System.out.println("Method = \"" + methodName
					+ "\", result = \"" + methodResult + "\"");
			}
			rs.close();

			System.out.println("*** DatabaseMetaData 'boolean' functions:");
			rs = stmt
				.executeQuery("SELECT * FROM javatest.getMetaDataBooleans()");
			while(rs.next())
			{
				String methodName = rs.getString(1);
				boolean methodResult = rs.getBoolean(2);
				System.out.println("Method = \"" + methodName
					+ "\", result = \"" + methodResult + "\"");
			}
			rs.close();

			System.out.println("*** DatabaseMetaData 'int' functions:");
			rs = stmt.executeQuery("SELECT * FROM javatest.getMetaDataInts()");
			while(rs.next())
			{
				String methodName = rs.getString(1);
				int methodResult = rs.getInt(2);
				System.out.println("Method = \"" + methodName
					+ "\", result = \"" + methodResult + "\"");
			}
			rs.close();

			executeMetaDataFunction(stmt,
				"getAttributes((String)null,\"javatest\",\"%\",\"%\")");
			executeMetaDataFunction(stmt,
				"getBestRowIdentifier((String)null,\"sqlj\",\"jar_repository\",0,FALSE)");
			executeMetaDataFunction(stmt, "getCatalogs()");
			executeMetaDataFunction(stmt,
				"getColumnPrivileges((String)null,\"sqlj\",\"jar_repository\",\"jarid\")");
			executeMetaDataFunction(stmt,
				"getColumns((String)null,\"sqlj\",\"jar_repository\",\"%\")");
			executeMetaDataFunction(
				stmt,
				"getCrossReference((String)null,\"sqlj\",\"jar_repository\",(String)null,\"sqlj\",\"jar_entry\")");
			executeMetaDataFunction(stmt,
				"getExportedKeys((String)null,\"sqlj\",\"jar_repository\")");
			executeMetaDataFunction(stmt,
				"getImportedKeys((String)null,\"sqlj\",\"jar_repository\")");
			executeMetaDataFunction(stmt,
				"getIndexInfo((String)null,\"sqlj\",\"jar_repository\",TRUE,FALSE)");
			executeMetaDataFunction(stmt,
				"getPrimaryKeys((String)null,\"sqlj\",\"jar_repository\")");
			executeMetaDataFunction(stmt,
				"getProcedureColumns((String)null,\"sqlj\",\"install_jar\",(String)null)");
			executeMetaDataFunction(stmt,
				"getProcedures((String)null,\"sqlj\",\"%\")");
			executeMetaDataFunction(stmt, "getSchemas()");
			executeMetaDataFunction(stmt,
				"getSuperTables((String)null,\"sqlj\",\"jar_repository\")");
			executeMetaDataFunction(stmt,
				"getSuperTypes((String)null,\"sqlj\",\"%\")");
			executeMetaDataFunction(stmt,
				"getTablePrivileges((String)null,\"sqlj\",\"jar_repository\")");
			executeMetaDataFunction(stmt,
				"getTables((String)null,\"sqlj\",\"jar%\",{\"TABLE\"})");
			executeMetaDataFunction(stmt, "getTableTypes()");
			executeMetaDataFunction(stmt, "getTypeInfo()");
			executeMetaDataFunction(stmt,
				"getUDTs((String)null,\"sqlj\",\"%\",(int[])null)");
			executeMetaDataFunction(stmt,
				"getVersionColumns((String)null,\"sqlj\",\"jar_repository\")");
		}
		finally
		{
			if(rs != null)
			{
				try
				{
					rs.close();
				}
				catch(SQLException e)
				{
				}
				rs = null;
			}
		}
	}

	private void executeMetaDataFunction(Statement stmt, String functionCall)
	throws SQLException
	{
		ResultSet rs = null;
		try
		{
			System.out.println("*** " + functionCall + ":");
			rs = stmt
				.executeQuery("SELECT * FROM javatest.callMetaDataMethod('"
					+ functionCall + "')");
			while(rs.next())
			{
				System.out.println(rs.getString(1));
			}
			rs.close();

		}
		catch(Exception e)
		{
			System.out.println("  Failed: " + e.getMessage());
		}
		finally
		{
			if(rs != null)
			{
				try
				{
					rs.close();
				}
				catch(SQLException e)
				{
				}
				rs = null;
			}
		}
	}

	public void testResultSet() throws SQLException
	{
		String sql;
		Statement stmt = m_connection.createStatement();
		ResultSet rs = null;

		try
		{
			System.out.println("*** ResultSet test:");
			sql = "SELECT * FROM javatest.executeSelect("
				+ "'select ''Foo'' as t_varchar, 1::integer as t_integer, "
				+ "1.5::float as t_float, 23.67::decimal(8,2) as t_decimal, "
				+ "''2005-06-01''::date as t_date, ''20:56''::time as t_time, "
				+ "''2006-02-04 23:55:10''::timestamp as t_timestamp')";

			rs = stmt.executeQuery(sql);
			System.out.println("SQL = " + sql);
			System.out.println("results:");
			while(rs.next())
			{
				System.out.println(rs.getString(1));
			}
			rs.close();
		}
		finally
		{
			if(rs != null)
			{
				try
				{
					rs.close();
				}
				catch(SQLException e)
				{
				}
				rs = null;
			}
		}
	}
}
