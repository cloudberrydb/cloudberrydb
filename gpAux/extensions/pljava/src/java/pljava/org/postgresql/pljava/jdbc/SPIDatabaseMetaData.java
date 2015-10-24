package org.postgresql.pljava.jdbc;

/**
 * @author Filip Hrbek
 */
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

import org.postgresql.pljava.internal.AclId;
import org.postgresql.pljava.internal.Backend;
import org.postgresql.pljava.internal.ExecutionPlan;
import org.postgresql.pljava.internal.Oid;

public class SPIDatabaseMetaData implements DatabaseMetaData
{
	public SPIDatabaseMetaData(SPIConnection conn)
	{
		m_connection = conn;
	}

	private static final String KEYWORDS = "abort,acl,add,aggregate,append,archive,"
		+ "arch_store,backward,binary,boolean,change,cluster,"
		+ "copy,database,delimiter,delimiters,do,extend,"
		+ "explain,forward,heavy,index,inherits,isnull,"
		+ "light,listen,load,merge,nothing,notify,"
		+ "notnull,oids,purge,rename,replace,retrieve,"
		+ "returns,rule,recipe,setof,stdin,stdout,store,"
		+ "vacuum,verbose,version";

	private final SPIConnection m_connection; // The connection association

	private static final int VARHDRSZ = 4; // length for int4

	private int NAMEDATALEN = 0; // length for name datatype

	private int INDEX_MAX_KEYS = 0; // maximum number of keys in an index.

	protected int getMaxIndexKeys() throws SQLException
	{
		if(INDEX_MAX_KEYS == 0)
			INDEX_MAX_KEYS = Integer.parseInt(Backend.getConfigOption("max_index_keys"));
		return INDEX_MAX_KEYS;
	}

	protected int getMaxNameLength() throws SQLException
	{
		if(NAMEDATALEN == 0)
		{
			String sql = "SELECT t.typlen " +
				         " FROM pg_catalog.pg_type t, pg_catalog.pg_namespace n" +
						 " WHERE t.typnamespace=n.oid" +
						 "   AND t.typname='name'" +
						 "   AND n.nspname='pg_catalog'";

			ResultSet rs = m_connection.createStatement().executeQuery(sql);
			if(!rs.next())
			{
				throw new SQLException(
                     "Unable to find name datatype in the system catalogs."); 
			}
			NAMEDATALEN = rs.getInt("typlen");
			rs.close();
		}
		return NAMEDATALEN - 1;
	}

	/*
	 * Can all the procedures returned by getProcedures be called by the current
	 * user? @return true if so @exception SQLException if a database access
	 * error occurs
	 */
	public boolean allProceduresAreCallable() throws SQLException
	{
		return true; // For now...
	}

	/*
	 * Can all the tables returned by getTable be SELECTed by the current user?
	 * @return true if so @exception SQLException if a database access error
	 * occurs
	 */
	public boolean allTablesAreSelectable() throws SQLException
	{
		return true; // For now...
	}

	/*
	 * What is the URL for this database? @return the url or null if it cannot
	 * be generated @exception SQLException if a database access error occurs
	 */
	public String getURL() throws SQLException
	{
		return "jdbc:default:connection";
	}

	/*
	 * What is our user name as known to the database? @return our database user
	 * name @exception SQLException if a database access error occurs
	 */
	public String getUserName() throws SQLException
	{
		return AclId.getUser().getName();
	}

	/*
	 * Is the database in read-only mode? @return true if so @exception
	 * SQLException if a database access error occurs
	 */
	public boolean isReadOnly() throws SQLException
	{
		return m_connection.isReadOnly();
	}

	/*
	 * Are NULL values sorted high? @return true if so @exception SQLException
	 * if a database access error occurs
	 */
	public boolean nullsAreSortedHigh() throws SQLException
	{
		return true;
	}

	/*
	 * Are NULL values sorted low? @return true if so @exception SQLException if
	 * a database access error occurs
	 */
	public boolean nullsAreSortedLow() throws SQLException
	{
		return false;
	}

	/*
	 * Are NULL values sorted at the start regardless of sort order? @return
	 * true if so @exception SQLException if a database access error occurs
	 */
	public boolean nullsAreSortedAtStart() throws SQLException
	{
		return false;
	}

	/*
	 * Are NULL values sorted at the end regardless of sort order? @return true
	 * if so @exception SQLException if a database access error occurs
	 */
	public boolean nullsAreSortedAtEnd() throws SQLException
	{
		return false;
	}

	/*
	 * What is the name of this database product - we hope that it is
	 * PostgreSQL, so we return that explicitly. @return the database product
	 * name @exception SQLException if a database access error occurs
	 */
	public String getDatabaseProductName() throws SQLException
	{
		return "PostgreSQL";
	}

	/*
	 * What is the version of this database product. @return the database
	 * version @exception SQLException if a database access error occurs
	 */
	public String getDatabaseProductVersion() throws SQLException
	{
		int[] ver = m_connection.getVersionNumber();
		return ver[0] + "." + ver[1] + "." + ver[2];
	}

	/*
	 * What is the name of this JDBC driver? If we don't know this we are doing
	 * something wrong! @return the JDBC driver name @exception SQLException
	 * why?
	 */
	public String getDriverName() throws SQLException
	{
		return "PostgreSQL pljava SPI Driver";
	}

	/*
	 * What is the version string of this JDBC driver? Again, this is static.
	 * @return the JDBC driver name. @exception SQLException why?
	 */
	public String getDriverVersion() throws SQLException
	{
		SPIDriver d = new SPIDriver();

		return d.getMajorVersion() + "." + d.getMinorVersion();
	}

	/*
	 * What is this JDBC driver's major version number? @return the JDBC driver
	 * major version
	 */
	public int getDriverMajorVersion()
	{
		return new SPIDriver().getMajorVersion();
	}

	/*
	 * What is this JDBC driver's minor version number? @return the JDBC driver
	 * minor version
	 */
	public int getDriverMinorVersion()
	{
		return new SPIDriver().getMinorVersion();
	}

	/*
	 * Does the database store tables in a local file? No - it stores them in a
	 * file on the server. @return true if so @exception SQLException if a
	 * database access error occurs
	 */
	public boolean usesLocalFiles() throws SQLException
	{
		return false;
	}

	/*
	 * Does the database use a file for each table? Well, not really, since it
	 * doesnt use local files. @return true if so @exception SQLException if a
	 * database access error occurs
	 */
	public boolean usesLocalFilePerTable() throws SQLException
	{
		return false;
	}

	/*
	 * Does the database treat mixed case unquoted SQL identifiers as case
	 * sensitive and as a result store them in mixed case? A JDBC-Compliant
	 * driver will always return false. @return true if so @exception
	 * SQLException if a database access error occurs
	 */
	public boolean supportsMixedCaseIdentifiers() throws SQLException
	{
		return false;
	}

	/*
	 * Does the database treat mixed case unquoted SQL identifiers as case
	 * insensitive and store them in upper case? @return true if so
	 */
	public boolean storesUpperCaseIdentifiers() throws SQLException
	{
		return false;
	}

	/*
	 * Does the database treat mixed case unquoted SQL identifiers as case
	 * insensitive and store them in lower case? @return true if so
	 */
	public boolean storesLowerCaseIdentifiers() throws SQLException
	{
		return true;
	}

	/*
	 * Does the database treat mixed case unquoted SQL identifiers as case
	 * insensitive and store them in mixed case? @return true if so
	 */
	public boolean storesMixedCaseIdentifiers() throws SQLException
	{
		return false;
	}

	/*
	 * Does the database treat mixed case quoted SQL identifiers as case
	 * sensitive and as a result store them in mixed case? A JDBC compliant
	 * driver will always return true. @return true if so @exception
	 * SQLException if a database access error occurs
	 */
	public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException
	{
		return true;
	}

	/*
	 * Does the database treat mixed case quoted SQL identifiers as case
	 * insensitive and store them in upper case? @return true if so
	 */
	public boolean storesUpperCaseQuotedIdentifiers() throws SQLException
	{
		return false;
	}

	/*
	 * Does the database treat mixed case quoted SQL identifiers as case
	 * insensitive and store them in lower case? @return true if so
	 */
	public boolean storesLowerCaseQuotedIdentifiers() throws SQLException
	{
		return false;
	}

	/*
	 * Does the database treat mixed case quoted SQL identifiers as case
	 * insensitive and store them in mixed case? @return true if so
	 */
	public boolean storesMixedCaseQuotedIdentifiers() throws SQLException
	{
		return false;
	}

	/*
	 * What is the string used to quote SQL identifiers? This returns a space if
	 * identifier quoting isn't supported. A JDBC Compliant driver will always
	 * use a double quote character. @return the quoting string @exception
	 * SQLException if a database access error occurs
	 */
	public String getIdentifierQuoteString() throws SQLException
	{
		return "\"";
	}

	/*
	 * Get a comma separated list of all a database's SQL keywords that are NOT
	 * also SQL92 keywords. <p>Within PostgreSQL, the keywords are found in
	 * src/backend/parser/keywords.c <p>For SQL Keywords, I took the list
	 * provided at <a
	 * href="http://web.dementia.org/~shadow/sql/sql3bnf.sep93.txt">
	 * http://web.dementia.org/~shadow/sql/sql3bnf.sep93.txt</a> which is for
	 * SQL3, not SQL-92, but it is close enough for this purpose. @return a
	 * comma separated list of keywords we use @exception SQLException if a
	 * database access error occurs
	 */
	public String getSQLKeywords() throws SQLException
	{
		return KEYWORDS;
	}

	/**
	 * get supported escaped numeric functions
	 * 
	 * @return a comma separated list of function names
	 */
	public String getNumericFunctions() throws SQLException
	{
		return BuiltinFunctions.ABS + ',' + BuiltinFunctions.ACOS + ','
			+ BuiltinFunctions.ASIN + ',' + BuiltinFunctions.ATAN + ','
			+ BuiltinFunctions.ATAN2 + ',' + BuiltinFunctions.CEILING + ','
			+ BuiltinFunctions.COS + ',' + BuiltinFunctions.COT + ','
			+ BuiltinFunctions.DEGREES + ',' + BuiltinFunctions.EXP + ','
			+ BuiltinFunctions.FLOOR + ',' + BuiltinFunctions.LOG + ','
			+ BuiltinFunctions.LOG10 + ',' + BuiltinFunctions.MOD + ','
			+ BuiltinFunctions.PI + ',' + BuiltinFunctions.POWER + ','
			+ BuiltinFunctions.RADIANS + ',' + BuiltinFunctions.RAND + ','
			+ BuiltinFunctions.ROUND + ',' + BuiltinFunctions.SIGN + ','
			+ BuiltinFunctions.SIN + ',' + BuiltinFunctions.SQRT + ','
			+ BuiltinFunctions.TAN + ',' + BuiltinFunctions.TRUNCATE;

	}

	public String getStringFunctions() throws SQLException
	{
		String funcs = BuiltinFunctions.ASCII + ',' + BuiltinFunctions.CHAR
			+ ',' + BuiltinFunctions.CONCAT + ',' + BuiltinFunctions.LCASE
			+ ',' + BuiltinFunctions.LEFT + ',' + BuiltinFunctions.LENGTH + ','
			+ BuiltinFunctions.LTRIM + ',' + BuiltinFunctions.REPEAT + ','
			+ BuiltinFunctions.RTRIM + ',' + BuiltinFunctions.SPACE + ','
			+ BuiltinFunctions.SUBSTRING + ',' + BuiltinFunctions.UCASE + ','
			+ BuiltinFunctions.REPLACE;

		// Currently these don't work correctly with parameterized
		// arguments, so leave them out. They reorder the arguments
		// when rewriting the query, but no translation layer is provided,
		// so a setObject(N, obj) will not go to the correct parameter.
		// ','+BuiltinFunctions.INSERT+','+BuiltinFunctions.LOCATE+
		// ','+BuiltinFunctions.RIGHT+
		return funcs;
	}

	public String getSystemFunctions() throws SQLException
	{
		return BuiltinFunctions.DATABASE + ',' + BuiltinFunctions.IFNULL
				+ ',' + BuiltinFunctions.USER;
	}

	public String getTimeDateFunctions() throws SQLException
	{
		return BuiltinFunctions.CURDATE + ',' + BuiltinFunctions.CURTIME + ','
			+ BuiltinFunctions.DAYNAME + ',' + BuiltinFunctions.DAYOFMONTH
			+ ',' + BuiltinFunctions.DAYOFWEEK + ','
			+ BuiltinFunctions.DAYOFYEAR + ',' + BuiltinFunctions.HOUR + ','
			+ BuiltinFunctions.MINUTE + ',' + BuiltinFunctions.MONTH + ','
			+ BuiltinFunctions.MONTHNAME + ',' + BuiltinFunctions.NOW + ','
			+ BuiltinFunctions.QUARTER + ',' + BuiltinFunctions.SECOND + ','
			+ BuiltinFunctions.WEEK + ',' + BuiltinFunctions.YEAR;
	}

	/*
	 * This is the string that can be used to escape '_' and '%' in a search
	 * string pattern style catalog search parameters @return the string used to
	 * escape wildcard characters @exception SQLException if a database access
	 * error occurs
	 */
	public String getSearchStringEscape() throws SQLException
	{
		// Java's parse takes off two backslashes
		// and then pg's input parser takes off another layer
		// so we need many backslashes here.
		//
		// This would work differently if you used a PreparedStatement
		// and " mycol LIKE ? " which using the V3 protocol would skip
		// pg's input parser, but I don't know what we can do about that.
		//
		return "\\";
	}

	/*
	 * Get all the "extra" characters that can be used in unquoted identifier
	 * names (those beyond a-zA-Z0-9 and _) <p>From the file
	 * src/backend/parser/scan.l, an identifier is {letter}{letter_or_digit}
	 * which makes it just those listed above. @return a string containing the
	 * extra characters @exception SQLException if a database access error
	 * occurs
	 */
	public String getExtraNameCharacters() throws SQLException
	{
		return "";
	}

	/*
	 * Is "ALTER TABLE" with an add column supported? Yes for PostgreSQL 6.1
	 * @return true if so @exception SQLException if a database access error
	 * occurs
	 */
	public boolean supportsAlterTableWithAddColumn() throws SQLException
	{
		return true;
	}

	/*
	 * Is "ALTER TABLE" with a drop column supported? @return true if so
	 * @exception SQLException if a database access error occurs
	 */
	public boolean supportsAlterTableWithDropColumn() throws SQLException
	{
		return true;
	}

	/*
	 * Is column aliasing supported? <p>If so, the SQL AS clause can be used to
	 * provide names for computed columns or to provide alias names for columns
	 * as required. A JDBC Compliant driver always returns true. <p>e.g. <br><pre>
	 * select count(C) as C_COUNT from T group by C; </pre><br> should return a
	 * column named as C_COUNT instead of count(C) @return true if so @exception
	 * SQLException if a database access error occurs
	 */
	public boolean supportsColumnAliasing() throws SQLException
	{
		return true;
	}

	/*
	 * Are concatenations between NULL and non-NULL values NULL? A JDBC
	 * Compliant driver always returns true @return true if so @exception
	 * SQLException if a database access error occurs
	 */
	public boolean nullPlusNonNullIsNull() throws SQLException
	{
		return true;
	}

	public boolean supportsConvert() throws SQLException
	{
		return false;
	}

	public boolean supportsConvert(int fromType, int toType)
	throws SQLException
	{
		return false;
	}

	/*
	 * Are table correlation names supported? A JDBC Compliant driver always
	 * returns true. @return true if so; false otherwise @exception SQLException -
	 * if a database access error occurs
	 */
	public boolean supportsTableCorrelationNames() throws SQLException
	{
		return true;
	}

	/*
	 * If table correlation names are supported, are they restricted to be
	 * different from the names of the tables? @return true if so; false
	 * otherwise @exception SQLException - if a database access error occurs
	 */
	public boolean supportsDifferentTableCorrelationNames() throws SQLException
	{
		return false;
	}

	/*
	 * Are expressions in "ORDER BY" lists supported? <br>e.g. select * from t
	 * order by a + b; @return true if so @exception SQLException if a database
	 * access error occurs
	 */
	public boolean supportsExpressionsInOrderBy() throws SQLException
	{
		return true;
	}

	/*
	 * Can an "ORDER BY" clause use columns not in the SELECT? @return true if
	 * so @exception SQLException if a database access error occurs
	 */
	public boolean supportsOrderByUnrelated() throws SQLException
	{
		return true;
	}

	/*
	 * Is some form of "GROUP BY" clause supported? I checked it, and yes it is.
	 * @return true if so @exception SQLException if a database access error
	 * occurs
	 */
	public boolean supportsGroupBy() throws SQLException
	{
		return true;
	}

	/*
	 * Can a "GROUP BY" clause use columns not in the SELECT? @return true if so
	 * @exception SQLException if a database access error occurs
	 */
	public boolean supportsGroupByUnrelated() throws SQLException
	{
		return true;
	}

	/*
	 * Can a "GROUP BY" clause add columns not in the SELECT provided it
	 * specifies all the columns in the SELECT? Does anyone actually understand
	 * what they mean here? (I think this is a subset of the previous function. --
	 * petere) @return true if so @exception SQLException if a database access
	 * error occurs
	 */
	public boolean supportsGroupByBeyondSelect() throws SQLException
	{
		return true;
	}

	/*
	 * Is the escape character in "LIKE" clauses supported? A JDBC compliant
	 * driver always returns true. @return true if so @exception SQLException if
	 * a database access error occurs
	 */
	public boolean supportsLikeEscapeClause() throws SQLException
	{
		return true;
	}

	/*
	 * Are multiple ResultSets from a single execute supported? Well, I
	 * implemented it, but I dont think this is possible from the back ends
	 * point of view. @return true if so @exception SQLException if a database
	 * access error occurs
	 */
	public boolean supportsMultipleResultSets() throws SQLException
	{
		return false;
	}

	/*
	 * Can we have multiple transactions open at once (on different
	 * connections?) I guess we can have, since Im relying on it. @return true
	 * if so @exception SQLException if a database access error occurs
	 */
	public boolean supportsMultipleTransactions() throws SQLException
	{
		return true;
	}

	/*
	 * Can columns be defined as non-nullable. A JDBC Compliant driver always
	 * returns true. <p>This changed from false to true in v6.2 of the driver,
	 * as this support was added to the backend. @return true if so @exception
	 * SQLException if a database access error occurs
	 */
	public boolean supportsNonNullableColumns() throws SQLException
	{
		return true;
	}

	/*
	 * Does this driver support the minimum ODBC SQL grammar. This grammar is
	 * defined at: <p><a
	 * href="http://www.microsoft.com/msdn/sdk/platforms/doc/odbc/src/intropr.htm">http://www.microsoft.com/msdn/sdk/platforms/doc/odbc/src/intropr.htm</a>
	 * <p>In Appendix C. From this description, we seem to support the ODBC
	 * minimal (Level 0) grammar. @return true if so @exception SQLException if
	 * a database access error occurs
	 */
	public boolean supportsMinimumSQLGrammar() throws SQLException
	{
		return true;
	}

	/*
	 * Does this driver support the Core ODBC SQL grammar. We need SQL-92
	 * conformance for this. @return true if so @exception SQLException if a
	 * database access error occurs
	 */
	public boolean supportsCoreSQLGrammar() throws SQLException
	{
		return false;
	}

	/*
	 * Does this driver support the Extended (Level 2) ODBC SQL grammar. We
	 * don't conform to the Core (Level 1), so we can't conform to the Extended
	 * SQL Grammar. @return true if so @exception SQLException if a database
	 * access error occurs
	 */
	public boolean supportsExtendedSQLGrammar() throws SQLException
	{
		return false;
	}

	/*
	 * Does this driver support the ANSI-92 entry level SQL grammar? All JDBC
	 * Compliant drivers must return true. We currently report false until
	 * 'schema' support is added. Then this should be changed to return true,
	 * since we will be mostly compliant (probably more compliant than many
	 * other databases) And since this is a requirement for all JDBC drivers we
	 * need to get to the point where we can return true. @return true if so
	 * @exception SQLException if a database access error occurs
	 */
	public boolean supportsANSI92EntryLevelSQL() throws SQLException
	{
		return true;
	}

	/*
	 * Does this driver support the ANSI-92 intermediate level SQL grammar?
	 * @return true if so @exception SQLException if a database access error
	 * occurs
	 */
	public boolean supportsANSI92IntermediateSQL() throws SQLException
	{
		return false;
	}

	/*
	 * Does this driver support the ANSI-92 full SQL grammar? @return true if so
	 * @exception SQLException if a database access error occurs
	 */
	public boolean supportsANSI92FullSQL() throws SQLException
	{
		return false;
	}

	/*
	 * Is the SQL Integrity Enhancement Facility supported? Our best guess is
	 * that this means support for constraints @return true if so @exception
	 * SQLException if a database access error occurs
	 */
	public boolean supportsIntegrityEnhancementFacility() throws SQLException
	{
		return true;
	}

	/*
	 * Is some form of outer join supported? @return true if so @exception
	 * SQLException if a database access error occurs
	 */
	public boolean supportsOuterJoins() throws SQLException
	{
		return true;
	}

	/*
	 * Are full nexted outer joins supported? @return true if so @exception
	 * SQLException if a database access error occurs
	 */
	public boolean supportsFullOuterJoins() throws SQLException
	{
		return true;
	}

	/*
	 * Is there limited support for outer joins? @return true if so @exception
	 * SQLException if a database access error occurs
	 */
	public boolean supportsLimitedOuterJoins() throws SQLException
	{
		return true;
	}

	/*
	 * What is the database vendor's preferred term for "schema"? PostgreSQL
	 * doesn't have schemas, but when it does, we'll use the term "schema".
	 * @return the vendor term @exception SQLException if a database access
	 * error occurs
	 */
	public String getSchemaTerm() throws SQLException
	{
		return "schema";
	}

	/*
	 * What is the database vendor's preferred term for "procedure"?
	 * Traditionally, "function" has been used. @return the vendor term
	 * @exception SQLException if a database access error occurs
	 */
	public String getProcedureTerm() throws SQLException
	{
		return "function";
	}

	/*
	 * What is the database vendor's preferred term for "catalog"? @return the
	 * vendor term @exception SQLException if a database access error occurs
	 */
	public String getCatalogTerm() throws SQLException
	{
		return "database";
	}

	/*
	 * Does a catalog appear at the start of a qualified table name? (Otherwise
	 * it appears at the end). @return true if so @exception SQLException if a
	 * database access error occurs
	 */
	public boolean isCatalogAtStart() throws SQLException
	{
		// return true here; we return false for every other catalog function
		// so it won't matter what we return here D.C.
		return true;
	}

	/*
	 * What is the Catalog separator. @return the catalog separator string
	 * @exception SQLException if a database access error occurs
	 */
	public String getCatalogSeparator() throws SQLException
	{
		// Give them something to work with here
		// everything else returns false so it won't matter what we return here
		// D.C.
		return ".";
	}

	/*
	 * Can a schema name be used in a data manipulation statement? @return true
	 * if so @exception SQLException if a database access error occurs
	 */
	public boolean supportsSchemasInDataManipulation() throws SQLException
	{
		return true;
	}

	/*
	 * Can a schema name be used in a procedure call statement? @return true if
	 * so @exception SQLException if a database access error occurs
	 */
	public boolean supportsSchemasInProcedureCalls() throws SQLException
	{
		return true;
	}

	/*
	 * Can a schema be used in a table definition statement? @return true if so
	 * @exception SQLException if a database access error occurs
	 */
	public boolean supportsSchemasInTableDefinitions() throws SQLException
	{
		return true;
	}

	/*
	 * Can a schema name be used in an index definition statement? @return true
	 * if so @exception SQLException if a database access error occurs
	 */
	public boolean supportsSchemasInIndexDefinitions() throws SQLException
	{
		return true;
	}

	/*
	 * Can a schema name be used in a privilege definition statement? @return
	 * true if so @exception SQLException if a database access error occurs
	 */
	public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException
	{
		return true;
	}

	/*
	 * Can a catalog name be used in a data manipulation statement? @return true
	 * if so @exception SQLException if a database access error occurs
	 */
	public boolean supportsCatalogsInDataManipulation() throws SQLException
	{
		return false;
	}

	/*
	 * Can a catalog name be used in a procedure call statement? @return true if
	 * so @exception SQLException if a database access error occurs
	 */
	public boolean supportsCatalogsInProcedureCalls() throws SQLException
	{
		return false;
	}

	/*
	 * Can a catalog name be used in a table definition statement? @return true
	 * if so @exception SQLException if a database access error occurs
	 */
	public boolean supportsCatalogsInTableDefinitions() throws SQLException
	{
		return false;
	}

	/*
	 * Can a catalog name be used in an index definition? @return true if so
	 * @exception SQLException if a database access error occurs
	 */
	public boolean supportsCatalogsInIndexDefinitions() throws SQLException
	{
		return false;
	}

	/*
	 * Can a catalog name be used in a privilege definition statement? @return
	 * true if so @exception SQLException if a database access error occurs
	 */
	public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException
	{
		return false;
	}

	/*
	 * We support cursors for gets only it seems. I dont see a method to get a
	 * positioned delete. @return true if so @exception SQLException if a
	 * database access error occurs
	 */
	public boolean supportsPositionedDelete() throws SQLException
	{
		return false; // For now...
	}

	/*
	 * Is positioned UPDATE supported? @return true if so @exception
	 * SQLException if a database access error occurs
	 */
	public boolean supportsPositionedUpdate() throws SQLException
	{
		return false; // For now...
	}

	/*
	 * Is SELECT for UPDATE supported? @return true if so; false otherwise
	 * @exception SQLException - if a database access error occurs
	 */
	public boolean supportsSelectForUpdate() throws SQLException
	{
		return true;
	}

	/*
	 * Are stored procedure calls using the stored procedure escape syntax
	 * supported? @return true if so; false otherwise @exception SQLException -
	 * if a database access error occurs
	 */
	public boolean supportsStoredProcedures() throws SQLException
	{
		return false;
	}

	/*
	 * Are subqueries in comparison expressions supported? A JDBC Compliant
	 * driver always returns true. @return true if so; false otherwise
	 * @exception SQLException - if a database access error occurs
	 */
	public boolean supportsSubqueriesInComparisons() throws SQLException
	{
		return true;
	}

	/*
	 * Are subqueries in 'exists' expressions supported? A JDBC Compliant driver
	 * always returns true. @return true if so; false otherwise @exception
	 * SQLException - if a database access error occurs
	 */
	public boolean supportsSubqueriesInExists() throws SQLException
	{
		return true;
	}

	/*
	 * Are subqueries in 'in' statements supported? A JDBC Compliant driver
	 * always returns true. @return true if so; false otherwise @exception
	 * SQLException - if a database access error occurs
	 */
	public boolean supportsSubqueriesInIns() throws SQLException
	{
		return true;
	}

	/*
	 * Are subqueries in quantified expressions supported? A JDBC Compliant
	 * driver always returns true. (No idea what this is, but we support a good
	 * deal of subquerying.) @return true if so; false otherwise @exception
	 * SQLException - if a database access error occurs
	 */
	public boolean supportsSubqueriesInQuantifieds() throws SQLException
	{
		return true;
	}

	/*
	 * Are correlated subqueries supported? A JDBC Compliant driver always
	 * returns true. (a.k.a. subselect in from?) @return true if so; false
	 * otherwise @exception SQLException - if a database access error occurs
	 */
	public boolean supportsCorrelatedSubqueries() throws SQLException
	{
		return true;
	}

	/*
	 * Is SQL UNION supported? @return true if so @exception SQLException if a
	 * database access error occurs
	 */
	public boolean supportsUnion() throws SQLException
	{
		return true;
	}

	/*
	 * Is SQL UNION ALL supported? @return true if so @exception SQLException if
	 * a database access error occurs
	 */
	public boolean supportsUnionAll() throws SQLException
	{
		return true;
	}

	/*
	 * In PostgreSQL, Cursors are only open within transactions. @return true if
	 * so @exception SQLException if a database access error occurs
	 */
	public boolean supportsOpenCursorsAcrossCommit() throws SQLException
	{
		return false;
	}

	/*
	 * Do we support open cursors across multiple transactions? @return true if
	 * so @exception SQLException if a database access error occurs
	 */
	public boolean supportsOpenCursorsAcrossRollback() throws SQLException
	{
		return false;
	}

	/*
	 * Can statements remain open across commits? They may, but this driver
	 * cannot guarentee that. In further reflection. we are talking a Statement
	 * object here, so the answer is yes, since the Statement is only a vehicle
	 * to ExecSQL() @return true if they always remain open; false otherwise
	 * @exception SQLException if a database access error occurs
	 */
	public boolean supportsOpenStatementsAcrossCommit() throws SQLException
	{
		return true;
	}

	/*
	 * Can statements remain open across rollbacks? They may, but this driver
	 * cannot guarentee that. In further contemplation, we are talking a
	 * Statement object here, so the answer is yes, since the Statement is only
	 * a vehicle to ExecSQL() in Connection @return true if they always remain
	 * open; false otherwise @exception SQLException if a database access error
	 * occurs
	 */
	public boolean supportsOpenStatementsAcrossRollback() throws SQLException
	{
		return true;
	}

	/*
	 * How many hex characters can you have in an inline binary literal @return
	 * the max literal length @exception SQLException if a database access error
	 * occurs
	 */
	public int getMaxBinaryLiteralLength() throws SQLException
	{
		return 0; // no limit
	}

	/*
	 * What is the maximum length for a character literal I suppose it is 8190
	 * (8192 - 2 for the quotes) @return the max literal length @exception
	 * SQLException if a database access error occurs
	 */
	public int getMaxCharLiteralLength() throws SQLException
	{
		return 0; // no limit
	}

	/*
	 * Whats the limit on column name length. @return the maximum column name
	 * length @exception SQLException if a database access error occurs
	 */
	public int getMaxColumnNameLength() throws SQLException
	{
		return getMaxNameLength();
	}

	/*
	 * What is the maximum number of columns in a "GROUP BY" clause? @return the
	 * max number of columns @exception SQLException if a database access error
	 * occurs
	 */
	public int getMaxColumnsInGroupBy() throws SQLException
	{
		return 0; // no limit
	}

	/*
	 * What's the maximum number of columns allowed in an index? @return max
	 * number of columns @exception SQLException if a database access error
	 * occurs
	 */
	public int getMaxColumnsInIndex() throws SQLException
	{
		return getMaxIndexKeys();
	}

	/*
	 * What's the maximum number of columns in an "ORDER BY clause? @return the
	 * max columns @exception SQLException if a database access error occurs
	 */
	public int getMaxColumnsInOrderBy() throws SQLException
	{
		return 0; // no limit
	}

	/*
	 * What is the maximum number of columns in a "SELECT" list? @return the max
	 * columns @exception SQLException if a database access error occurs
	 */
	public int getMaxColumnsInSelect() throws SQLException
	{
		return 0; // no limit
	}

	/*
	 * What is the maximum number of columns in a table? From the CREATE TABLE
	 * reference page... <p>"The new class is created as a heap with no initial
	 * data. A class can have no more than 1600 attributes (realistically, this
	 * is limited by the fact that tuple sizes must be less than 8192 bytes)..."
	 * @return the max columns @exception SQLException if a database access
	 * error occurs
	 */
	public int getMaxColumnsInTable() throws SQLException
	{
		return 1600;
	}

	/*
	 * How many active connection can we have at a time to this database? Well,
	 * since it depends on postmaster, which just does a listen() followed by an
	 * accept() and fork(), its basically very high. Unless the system runs out
	 * of processes, it can be 65535 (the number of aux. ports on a TCP/IP
	 * system). I will return 8192 since that is what even the largest system
	 * can realistically handle, @return the maximum number of connections
	 * @exception SQLException if a database access error occurs
	 */
	public int getMaxConnections() throws SQLException
	{
		return 8192;
	}

	/*
	 * What is the maximum cursor name length @return max cursor name length in
	 * bytes @exception SQLException if a database access error occurs
	 */
	public int getMaxCursorNameLength() throws SQLException
	{
		return getMaxNameLength();
	}

	/*
	 * Retrieves the maximum number of bytes for an index, including all of the
	 * parts of the index. @return max index length in bytes, which includes the
	 * composite of all the constituent parts of the index; a result of zero
	 * means that there is no limit or the limit is not known @exception
	 * SQLException if a database access error occurs
	 */
	public int getMaxIndexLength() throws SQLException
	{
		return 0; // no limit (larger than an int anyway)
	}

	public int getMaxSchemaNameLength() throws SQLException
	{
		return getMaxNameLength();
	}

	/*
	 * What is the maximum length of a procedure name @return the max name
	 * length in bytes @exception SQLException if a database access error occurs
	 */
	public int getMaxProcedureNameLength() throws SQLException
	{
		return getMaxNameLength();
	}

	public int getMaxCatalogNameLength() throws SQLException
	{
		return getMaxNameLength();
	}

	/*
	 * What is the maximum length of a single row? @return max row size in bytes
	 * @exception SQLException if a database access error occurs
	 */
	public int getMaxRowSize() throws SQLException
	{
		return 1073741824; // 1 GB
	}

	/*
	 * Did getMaxRowSize() include LONGVARCHAR and LONGVARBINARY blobs? We don't
	 * handle blobs yet @return true if so @exception SQLException if a database
	 * access error occurs
	 */
	public boolean doesMaxRowSizeIncludeBlobs() throws SQLException
	{
		return false;
	}

	/*
	 * What is the maximum length of a SQL statement? @return max length in
	 * bytes @exception SQLException if a database access error occurs
	 */
	public int getMaxStatementLength() throws SQLException
	{
		return 0; // actually whatever fits in size_t
	}

	/*
	 * How many active statements can we have open at one time to this database?
	 * Basically, since each Statement downloads the results as the query is
	 * executed, we can have many. However, we can only really have one
	 * statement per connection going at once (since they are executed serially) -
	 * so we return one. @return the maximum @exception SQLException if a
	 * database access error occurs
	 */
	public int getMaxStatements() throws SQLException
	{
		return 1;
	}

	/*
	 * What is the maximum length of a table name @return max name length in
	 * bytes @exception SQLException if a database access error occurs
	 */
	public int getMaxTableNameLength() throws SQLException
	{
		return getMaxNameLength();
	}

	/*
	 * What is the maximum number of tables that can be specified in a SELECT?
	 * @return the maximum @exception SQLException if a database access error
	 * occurs
	 */
	public int getMaxTablesInSelect() throws SQLException
	{
		return 0; // no limit
	}

	/*
	 * What is the maximum length of a user name @return the max name length in
	 * bytes @exception SQLException if a database access error occurs
	 */
	public int getMaxUserNameLength() throws SQLException
	{
		return getMaxNameLength();
	}

	/*
	 * What is the database's default transaction isolation level? We do not
	 * support this, so all transactions are SERIALIZABLE. @return the default
	 * isolation level @exception SQLException if a database access error occurs
	 * 
	 * @see Connection
	 */
	public int getDefaultTransactionIsolation() throws SQLException
	{
		return Connection.TRANSACTION_READ_COMMITTED;
	}

	/*
	 * Are transactions supported? If not, commit and rollback are noops and the
	 * isolation level is TRANSACTION_NONE. We do support transactions. @return
	 * true if transactions are supported @exception SQLException if a database
	 * access error occurs
	 */
	public boolean supportsTransactions() throws SQLException
	{
		return true;
	}

	/*
	 * Does the database support the given transaction isolation level? We only
	 * support TRANSACTION_SERIALIZABLE and TRANSACTION_READ_COMMITTED @param
	 * level the values are defined in java.sql.Connection @return true if so
	 * @exception SQLException if a database access error occurs
	 * 
	 * @see Connection
	 */
	public boolean supportsTransactionIsolationLevel(int level)
	throws SQLException
	{
		if(level == Connection.TRANSACTION_SERIALIZABLE
		|| level == Connection.TRANSACTION_READ_COMMITTED)
			return true;

		if(this.getDatabaseMajorVersion() >= 8
		&& (level == Connection.TRANSACTION_READ_UNCOMMITTED || level == Connection.TRANSACTION_REPEATABLE_READ))
			return true;

		return false;
	}

	/*
	 * Are both data definition and data manipulation transactions supported?
	 * @return true if so @exception SQLException if a database access error
	 * occurs
	 */
	public boolean supportsDataDefinitionAndDataManipulationTransactions()
	throws SQLException
	{
		return true;
	}

	/*
	 * Are only data manipulation statements withing a transaction supported?
	 * @return true if so @exception SQLException if a database access error
	 * occurs
	 */
	public boolean supportsDataManipulationTransactionsOnly()
	throws SQLException
	{
		return false;
	}

	/*
	 * Does a data definition statement within a transaction force the
	 * transaction to commit? I think this means something like: <p><pre>
	 * CREATE TABLE T (A INT); INSERT INTO T (A) VALUES (2); BEGIN; UPDATE T SET
	 * A = A + 1; CREATE TABLE X (A INT); SELECT A FROM T INTO X; COMMIT; </pre><p>
	 * does the CREATE TABLE call cause a commit? The answer is no. @return true
	 * if so @exception SQLException if a database access error occurs
	 */
	public boolean dataDefinitionCausesTransactionCommit() throws SQLException
	{
		return false;
	}

	/*
	 * Is a data definition statement within a transaction ignored? @return true
	 * if so @exception SQLException if a database access error occurs
	 */
	public boolean dataDefinitionIgnoredInTransactions() throws SQLException
	{
		return false;
	}

	/**
	 * Escape single quotes with another single quote.
	 */
	private static String escapeQuotes(String s)
	{
        if (s == null)
        {
            return null;
        }

		StringBuffer sb = new StringBuffer();
		int length = s.length();
		char prevChar = ' ';
		char prevPrevChar = ' ';
		for(int i = 0; i < length; i++)
		{
			char c = s.charAt(i);
			sb.append(c);
			if(c == '\''
				&& (prevChar != '\\' || (prevChar == '\\' && prevPrevChar == '\\')))
			{
				sb.append("'");
			}
			prevPrevChar = prevChar;
			prevChar = c;
		}
		return sb.toString();
	}

	/**
	 * Creates a condition with the specified operator
     * based on schema specification:<BR>
     * <UL>
     * <LI>schema is specified => search in this schema only</LI>
     * <LI>schema is equal to "" => search in the 'public' schema</LI>
     * <LI>schema is null =>  search in all schemas</LI>
	 * </UL>
	 */
	private static String resolveSchemaConditionWithOperator(
        String expr, String schema, String operator)
	{
        //schema is null => search in current_schemas(true)
        if (schema == null)
        {
			//This means that only "visible" schemas are searched.
			//It was approved to change to *all* schemas.
            //return expr + " " + operator + " ANY (current_schemas(true))";
			return "1=1";
        }
        //schema is specified => search in this schema
		else if(!"".equals(schema))
		{
			return expr + " " + operator + " '" + escapeQuotes(schema) + "' ";
		}
        //schema is "" => search in the 'public' schema
        else
        {
            return expr + " " + operator + " 'public' ";
        }
    }

	/**
	 * Creates an equality condition based on schema specification:<BR>
     * <UL>
     * <LI>schema is specified => search in this schema only</LI>
     * <LI>schema is equal to "" => search in the 'public' schema</LI>
     * <LI>schema is null =>  search in all schemas</LI>
	 * </UL>
	 */
	private static String resolveSchemaCondition(String expr, String schema)
	{
        return resolveSchemaConditionWithOperator(expr, schema, "=");
    }

	/**
	 * Creates a pattern condition based on schema specification:<BR>
     * <UL>
     * <LI>schema is specified => search in this schema only</LI>
     * <LI>schema is equal to "" => search in the 'public' schema</LI>
     * <LI>schema is null =>  search in all schemas</LI>
	 * </UL>
	 */
	private static String resolveSchemaPatternCondition(
        String expr, String schema)
	{
        return resolveSchemaConditionWithOperator(expr, schema, "LIKE");
    }

    /*
	 * Get a description of stored procedures available in a catalog <p>Only
	 * procedure descriptions matching the schema and procedure name criteria
	 * are returned. They are ordered by PROCEDURE_SCHEM and PROCEDURE_NAME <p>Each
	 * procedure description has the following columns: <ol> <li><b>PROCEDURE_CAT</b>
	 * String => procedure catalog (may be null) <li><b>PROCEDURE_SCHEM</b>
	 * String => procedure schema (may be null) <li><b>PROCEDURE_NAME</b>
	 * String => procedure name <li><b>ResultSetField 4</b> reserved (make it
	 * null) <li><b>ResultSetField 5</b> reserved (make it null) <li><b>ResultSetField
	 * 6</b> reserved (make it null) <li><b>REMARKS</b> String => explanatory
	 * comment on the procedure <li><b>PROCEDURE_TYPE</b> short => kind of
	 * procedure <ul> <li> procedureResultUnknown - May return a result <li>
	 * procedureNoResult - Does not return a result <li> procedureReturnsResult -
	 * Returns a result </ul> </ol> @param catalog - a catalog name; ""
	 * retrieves those without a catalog; null means drop catalog name from
	 * criteria @param schemaParrern - a schema name pattern; "" retrieves those
	 * without a schema - we ignore this parameter @param procedureNamePattern -
	 * a procedure name pattern @return ResultSet - each row is a procedure
	 * description @exception SQLException if a database access error occurs
	 */
	public java.sql.ResultSet getProcedures(String catalog,
		String schemaPattern, String procedureNamePattern) throws SQLException
	{
		String sql = "SELECT NULL AS PROCEDURE_CAT, n.nspname AS PROCEDURE_SCHEM, p.proname AS PROCEDURE_NAME, NULL, NULL, NULL, d.description AS REMARKS, "
				+ java.sql.DatabaseMetaData.procedureReturnsResult
				+ " AS PROCEDURE_TYPE "
				+ " FROM pg_catalog.pg_namespace n, pg_catalog.pg_proc p "
				+ " LEFT JOIN pg_catalog.pg_description d ON (p.oid=d.objoid) "
				+ " LEFT JOIN pg_catalog.pg_class c ON (d.classoid=c.oid AND c.relname='pg_proc') "
				+ " LEFT JOIN pg_catalog.pg_namespace pn ON (c.relnamespace=pn.oid AND pn.nspname='pg_catalog') "
				+ " WHERE p.pronamespace=n.oid "
                + " AND " + resolveSchemaPatternCondition(
                                "n.nspname", schemaPattern);
		if(procedureNamePattern != null)
		{
			sql += " AND p.proname LIKE '"
				+ escapeQuotes(procedureNamePattern) + "' ";
		}
		sql += " ORDER BY PROCEDURE_SCHEM, PROCEDURE_NAME ";
		return createMetaDataStatement().executeQuery(sql);
	}

	/*
	 * Get a description of a catalog's stored procedure parameters and result
	 * columns. <p>Only descriptions matching the schema, procedure and
	 * parameter name criteria are returned. They are ordered by PROCEDURE_SCHEM
	 * and PROCEDURE_NAME. Within this, the return value, if any, is first. Next
	 * are the parameter descriptions in call order. The column descriptions
	 * follow in column number order. <p>Each row in the ResultSet is a
	 * parameter description or column description with the following fields:
	 * <ol> <li><b>PROCEDURE_CAT</b> String => procedure catalog (may be null)
	 * <li><b>PROCEDURE_SCHE</b>M String => procedure schema (may be null)
	 * <li><b>PROCEDURE_NAME</b> String => procedure name <li><b>COLUMN_NAME</b>
	 * String => column/parameter name <li><b>COLUMN_TYPE</b> Short => kind of
	 * column/parameter: <ul><li>procedureColumnUnknown - nobody knows <li>procedureColumnIn -
	 * IN parameter <li>procedureColumnInOut - INOUT parameter <li>procedureColumnOut -
	 * OUT parameter <li>procedureColumnReturn - procedure return value <li>procedureColumnResult -
	 * result column in ResultSet </ul> <li><b>DATA_TYPE</b> short => SQL type
	 * from java.sql.Types <li><b>TYPE_NAME</b> String => Data source specific
	 * type name <li><b>PRECISION</b> int => precision <li><b>LENGTH</b> int =>
	 * length in bytes of data <li><b>SCALE</b> short => scale <li><b>RADIX</b>
	 * short => radix <li><b>NULLABLE</b> short => can it contain NULL? <ul><li>procedureNoNulls -
	 * does not allow NULL values <li>procedureNullable - allows NULL values
	 * <li>procedureNullableUnknown - nullability unknown <li><b>REMARKS</b>
	 * String => comment describing parameter/column </ol> @param catalog This
	 * is ignored in org.postgresql, advise this is set to null @param
	 * schemaPattern @param procedureNamePattern a procedure name pattern @param
	 * columnNamePattern a column name pattern, this is currently ignored
	 * because postgresql does not name procedure parameters. @return each row
	 * is a stored procedure parameter or column description @exception
	 * SQLException if a database-access error occurs
	 * 
	 * @see #getSearchStringEscape
	 */
	// Implementation note: This is required for Borland's JBuilder to work
	public java.sql.ResultSet getProcedureColumns(String catalog,
		String schemaPattern, String procedureNamePattern,
		String columnNamePattern) throws SQLException
	{
		ResultSetField f[] = new ResultSetField[13];
		ArrayList v = new ArrayList(); // The new ResultSet tuple stuff

		f[0] = new ResultSetField("PROCEDURE_CAT", TypeOid.VARCHAR,
			getMaxNameLength());
		f[1] = new ResultSetField("PROCEDURE_SCHEM", TypeOid.VARCHAR,
			getMaxNameLength());
		f[2] = new ResultSetField("PROCEDURE_NAME", TypeOid.VARCHAR,
			getMaxNameLength());
		f[3] = new ResultSetField("COLUMN_NAME", TypeOid.VARCHAR,
			getMaxNameLength());
		f[4] = new ResultSetField("COLUMN_TYPE", TypeOid.INT2, 2);
		f[5] = new ResultSetField("DATA_TYPE", TypeOid.INT2, 2);
		f[6] = new ResultSetField("TYPE_NAME", TypeOid.VARCHAR,
			getMaxNameLength());
		f[7] = new ResultSetField("PRECISION", TypeOid.INT4, 4);
		f[8] = new ResultSetField("LENGTH", TypeOid.INT4, 4);
		f[9] = new ResultSetField("SCALE", TypeOid.INT2, 2);
		f[10] = new ResultSetField("RADIX", TypeOid.INT2, 2);
		f[11] = new ResultSetField("NULLABLE", TypeOid.INT2, 2);
		f[12] = new ResultSetField("REMARKS", TypeOid.VARCHAR,
			getMaxNameLength());

		String sql = "SELECT n.nspname,p.proname,p.prorettype,p.proargtypes, t.typtype::varchar,t.typrelid "
				+ " FROM pg_catalog.pg_proc p,pg_catalog.pg_namespace n, pg_catalog.pg_type t "
				+ " WHERE p.pronamespace=n.oid AND p.prorettype=t.oid "
                + " AND " + resolveSchemaPatternCondition(
                                "n.nspname", schemaPattern);
		if(procedureNamePattern != null)
		{
			sql += " AND p.proname LIKE '"
				+ escapeQuotes(procedureNamePattern) + "' ";
		}
		sql += " ORDER BY n.nspname, p.proname ";

		ResultSet rs = m_connection.createStatement().executeQuery(sql);
		String schema = null;
		String procedureName = null;
		Oid returnType = null;
		String returnTypeType = null;
		Oid returnTypeRelid = null;

		Oid[] argTypes = null;
		while(rs.next())
		{
			schema = rs.getString("nspname");
			procedureName = rs.getString("proname");
			returnType = (Oid)rs.getObject("prorettype");
			returnTypeType = rs.getString("typtype");
			returnTypeRelid = (Oid)rs.getObject("typrelid");
			argTypes = (Oid[])rs.getObject("proargtypes");

			// decide if we are returning a single column result.
			if(!returnTypeType.equals("c"))
			{
				Object[] tuple = new Object[13];
				tuple[0] = null;
				tuple[1] = schema;
				tuple[2] = procedureName;
				tuple[3] = "returnValue";
				tuple[4] = new Short((short)java.sql.DatabaseMetaData.procedureColumnReturn);
				tuple[5] = new Short((short)m_connection.getSQLType(returnType));
				tuple[6] = m_connection.getPGType(returnType);
				tuple[7] = null;
				tuple[8] = null;
				tuple[9] = null;
				tuple[10] = null;
				tuple[11] = new Short((short)java.sql.DatabaseMetaData.procedureNullableUnknown);
				tuple[12] = null;
				v.add(tuple);
			}

			// Add a row for each argument.
			for(int i = 0; i < argTypes.length; i++)
			{
				Oid argOid = argTypes[i];
				Object[] tuple = new Object[13];
				tuple[0] = null;
				tuple[1] = schema;
				tuple[2] = procedureName;
				tuple[3] = "$" + (i + 1);
				tuple[4] = new Short((short)java.sql.DatabaseMetaData.procedureColumnIn);
				tuple[5] = new Short((short)m_connection.getSQLType(argOid));
				tuple[6] = m_connection.getPGType(argOid);
				tuple[7] = null;
				tuple[8] = null;
				tuple[9] = null;
				tuple[10] = null;
				tuple[11] = new Short((short)java.sql.DatabaseMetaData.procedureNullableUnknown);
				tuple[12] = null;
				v.add(tuple);
			}

			// if we are returning a multi-column result.
			if(returnTypeType.equals("c"))
			{
				String columnsql = "SELECT a.attname,a.atttypid FROM pg_catalog.pg_attribute a WHERE a.attrelid = ? ORDER BY a.attnum ";
				PreparedStatement stmt = m_connection.prepareStatement(columnsql);
				stmt.setObject(1, returnTypeRelid);
				ResultSet columnrs = stmt.executeQuery(columnsql);

				while(columnrs.next())
				{
					Oid columnTypeOid = (Oid)columnrs.getObject("atttypid");
					Object[] tuple = new Object[13];
					tuple[0] = null;
					tuple[1] = schema;
					tuple[2] = procedureName;
					tuple[3] = columnrs.getString("attname");
					tuple[4] = new Short((short)java.sql.DatabaseMetaData.procedureColumnResult);
					tuple[5] = new Short((short)m_connection.getSQLType(columnTypeOid));
					tuple[6] = m_connection.getPGType(columnTypeOid);
					tuple[7] = null;
					tuple[8] = null;
					tuple[9] = null;
					tuple[10] = null;
					tuple[11] = new Short((short)java.sql.DatabaseMetaData.procedureNullableUnknown);
					tuple[12] = null;
					v.add(tuple);
				}
				columnrs.close();
				stmt.close();
			}
		}
		rs.close();

		return createSyntheticResultSet(f, v);
	}

	/*
	 * Get a description of tables available in a catalog. <p>Only table
	 * descriptions matching the catalog, schema, table name and type criteria
	 * are returned. They are ordered by TABLE_TYPE, TABLE_SCHEM and TABLE_NAME.
	 * <p>Each table description has the following columns: <ol> <li><b>TABLE_CAT</b>
	 * String => table catalog (may be null) <li><b>TABLE_SCHEM</b> String =>
	 * table schema (may be null) <li><b>TABLE_NAME</b> String => table name
	 * <li><b>TABLE_TYPE</b> String => table type. Typical types are "TABLE",
	 * "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS",
	 * "SYNONYM". <li><b>REMARKS</b> String => explanatory comment on the
	 * table </ol> <p>The valid values for the types parameter are: "TABLE",
	 * "INDEX", "SEQUENCE", "VIEW", "SYSTEM TABLE", "SYSTEM INDEX", "SYSTEM
	 * VIEW", "SYSTEM TOAST TABLE", "SYSTEM TOAST INDEX", "TEMPORARY TABLE", and
	 * "TEMPORARY VIEW" @param catalog a catalog name; For org.postgresql, this
	 * is ignored, and should be set to null @param schemaPattern a schema name
	 * pattern @param tableNamePattern a table name pattern. For all tables this
	 * should be "%" @param types a list of table types to include; null returns
	 * all types @return each row is a table description @exception SQLException
	 * if a database-access error occurs.
	 */
	public java.sql.ResultSet getTables(String catalog, String schemaPattern,
		String tableNamePattern, String types[]) throws SQLException
	{
		String useSchemas = "SCHEMAS";
		String select = "SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM, c.relname AS TABLE_NAME, "
				+ " CASE n.nspname LIKE 'pg!_%' ESCAPE '!' OR n.nspname = 'information_schema' "
				+ " WHEN true THEN CASE "
				+ " WHEN n.nspname = 'pg_catalog' OR n.nspname = 'information_schema' THEN CASE c.relkind "
				+ "  WHEN 'r' THEN 'SYSTEM TABLE' "
				+ "  WHEN 'v' THEN 'SYSTEM VIEW' "
				+ "  WHEN 'i' THEN 'SYSTEM INDEX' "
				+ "  ELSE NULL "
				+ "  END "
				+ " WHEN n.nspname = 'pg_toast' THEN CASE c.relkind "
				+ "  WHEN 'r' THEN 'SYSTEM TOAST TABLE' "
				+ "  WHEN 'i' THEN 'SYSTEM TOAST INDEX' "
				+ "  ELSE NULL "
				+ "  END "
				+ " ELSE CASE c.relkind "
				+ "  WHEN 'r' THEN 'TEMPORARY TABLE' "
				+ "  WHEN 'i' THEN 'TEMPORARY INDEX' "
				+ "  ELSE NULL "
				+ "  END "
				+ " END "
				+ " WHEN false THEN CASE c.relkind "
				+ " WHEN 'r' THEN 'TABLE' "
				+ " WHEN 'i' THEN 'INDEX' "
				+ " WHEN 'S' THEN 'SEQUENCE' "
				+ " WHEN 'v' THEN 'VIEW' "
				+ " ELSE NULL "
				+ " END "
				+ " ELSE NULL "
				+ " END "
				+ " AS TABLE_TYPE, d.description AS REMARKS "
				+ " FROM pg_catalog.pg_namespace n, pg_catalog.pg_class c "
				+ " LEFT JOIN pg_catalog.pg_description d ON (c.oid = d.objoid AND d.objsubid = 0) "
				+ " LEFT JOIN pg_catalog.pg_class dc ON (d.classoid=dc.oid AND dc.relname='pg_class') "
				+ " LEFT JOIN pg_catalog.pg_namespace dn ON (dn.oid=dc.relnamespace AND dn.nspname='pg_catalog') "
				+ " WHERE c.relnamespace = n.oid "
                + " AND " + resolveSchemaPatternCondition(
                                "n.nspname", schemaPattern);
		String orderby = " ORDER BY TABLE_TYPE,TABLE_SCHEM,TABLE_NAME ";

		if(types == null)
		{
			types = s_defaultTableTypes;
		}
		if(tableNamePattern != null)
		{
			select += " AND c.relname LIKE '" + escapeQuotes(tableNamePattern)
				+ "' ";
		}
		String sql = select;
		sql += " AND (false ";
		for(int i = 0; i < types.length; i++)
		{
			HashMap clauses = (HashMap)s_tableTypeClauses.get(types[i]);
			if(clauses != null)
			{
				String clause = (String)clauses.get(useSchemas);
				sql += " OR ( " + clause + " ) ";
			}
		}
		sql += ") ";
		sql += orderby;

		return createMetaDataStatement().executeQuery(sql);
	}

	private static final HashMap s_tableTypeClauses;
	static
	{
		s_tableTypeClauses = new HashMap();
		HashMap ht = new HashMap();
		s_tableTypeClauses.put("TABLE", ht);
		ht.put("SCHEMAS",
			"c.relkind = 'r' AND n.nspname NOT LIKE 'pg!_%' ESCAPE '!' AND n.nspname <> 'information_schema'");
		ht.put("NOSCHEMAS",
			"c.relkind = 'r' AND c.relname NOT LIKE 'pg!_%' ESCAPE '!'");
		ht = new HashMap();
		s_tableTypeClauses.put("VIEW", ht);
		ht.put("SCHEMAS",
			"c.relkind = 'v' AND n.nspname <> 'pg_catalog' AND n.nspname <> 'information_schema'");
		ht.put("NOSCHEMAS",
			"c.relkind = 'v' AND c.relname NOT LIKE 'pg!_%' ESCAPE '!'");
		ht = new HashMap();
		s_tableTypeClauses.put("INDEX", ht);
		ht.put("SCHEMAS",
			"c.relkind = 'i' AND n.nspname NOT LIKE 'pg!_%' ESCAPE '!' AND n.nspname <> 'information_schema'");
		ht.put("NOSCHEMAS",
			"c.relkind = 'i' AND c.relname NOT LIKE 'pg!_%' ESCAPE '!'");
		ht = new HashMap();
		s_tableTypeClauses.put("SEQUENCE", ht);
		ht.put("SCHEMAS", "c.relkind = 'S'");
		ht.put("NOSCHEMAS", "c.relkind = 'S'");
		ht = new HashMap();
		s_tableTypeClauses.put("SYSTEM TABLE", ht);
		ht.put("SCHEMAS",
			"c.relkind = 'r' AND (n.nspname = 'pg_catalog' OR n.nspname = 'information_schema')");
		ht.put("NOSCHEMAS",
			"c.relkind = 'r' AND c.relname LIKE 'pg!_%' ESCAPE '!' AND c.relname NOT LIKE 'pgLIKE 'pg!_toast!_%' ESCAPE '!'toast!_%' ESCAPE '!' AND c.relname NOT LIKE 'pg!_temp!_%' ESCAPE '!'");
		ht = new HashMap();
		s_tableTypeClauses.put("SYSTEM TOAST TABLE", ht);
		ht.put("SCHEMAS", "c.relkind = 'r' AND n.nspname = 'pg_toast'");
		ht.put("NOSCHEMAS",
			"c.relkind = 'r' AND c.relname LIKE 'pg!_toast!_%' ESCAPE '!'");
		ht = new HashMap();
		s_tableTypeClauses.put("SYSTEM TOAST INDEX", ht);
		ht.put("SCHEMAS", "c.relkind = 'i' AND n.nspname = 'pg_toast'");
		ht.put("NOSCHEMAS",
			"c.relkind = 'i' AND c.relname LIKE 'pg!_toast!_%' ESCAPE '!'");
		ht = new HashMap();
		s_tableTypeClauses.put("SYSTEM VIEW", ht);
		ht.put("SCHEMAS",
			"c.relkind = 'v' AND (n.nspname = 'pg_catalog' OR n.nspname = 'information_schema') ");
		ht.put("NOSCHEMAS", "c.relkind = 'v' AND c.relname LIKE 'pg!_%' ESCAPE '!'");
		ht = new HashMap();
		s_tableTypeClauses.put("SYSTEM INDEX", ht);
		ht.put("SCHEMAS",
			"c.relkind = 'i' AND (n.nspname = 'pg_catalog' OR n.nspname = 'information_schema') ");
		ht.put("NOSCHEMAS",
			"c.relkind = 'v' AND c.relname LIKE 'pg!_%' ESCAPE '!' AND c.relname NOT LIKE 'pg!_toast!_%' ESCAPE '!' AND c.relname NOT LIKE 'pg!_temp!_%' ESCAPE '!'");
		ht = new HashMap();
		s_tableTypeClauses.put("TEMPORARY TABLE", ht);
		ht.put("SCHEMAS",
			"c.relkind = 'r' AND n.nspname LIKE 'pg!_temp!_%' ESCAPE '!' ");
		ht.put("NOSCHEMAS",
			"c.relkind = 'r' AND c.relname LIKE 'pg!_temp!_%' ESCAPE '!' ");
		ht = new HashMap();
		s_tableTypeClauses.put("TEMPORARY INDEX", ht);
		ht.put("SCHEMAS",
			"c.relkind = 'i' AND n.nspname LIKE 'pg!_temp!_%' ESCAPE '!' ");
		ht.put("NOSCHEMAS",
			"c.relkind = 'i' AND c.relname LIKE 'pg!_temp!_%' ESCAPE '!' ");
	}

	// These are the default tables, used when NULL is passed to getTables
	// The choice of these provide the same behaviour as psql's \d
	private static final String s_defaultTableTypes[] = { "TABLE", "VIEW",
		"INDEX", "SEQUENCE", "TEMPORARY TABLE" };

	/**
	 * Retrieves the schema names available in this database. The results are
	 * ordered by TABLE_CATALOG and TABLE_SCHEM.
	 *
	 * The schema columns are:
	 *   1. TABLE_SCHEM String => schema name
     *   2. TABLE_CATALOG String => catalog name (may be null) 
	 *
	 * @return a ResultSet object in which each row is a schema description 
	 * @throw SQLException - if a database access error occurs
	 */
	public java.sql.ResultSet getSchemas() 
	throws SQLException
	{
		String sql = 
			"SELECT nspname AS TABLE_SCHEM, null AS TABLE_CATALOG " +
			"FROM pg_catalog.pg_namespace " +
			"WHERE nspname !~ '^(pg_toast|pg_temp)' " +
			"ORDER BY TABLE_SCHEM";
		return createMetaDataStatement().executeQuery(sql);
	}


	/**
	 * Retrieves the schema names available in this database. The results are
	 * ordered by TABLE_CATALOG and TABLE_SCHEM.
	 *
	 * The schema columns are:
	 *
     * 1. TABLE_SCHEM String => schema name
     * 2. TABLE_CATALOG String => catalog name (may be null) 
	 *
	 * @param catalog - a catalog name; must match the catalog name as it is
	 *        stored in the database;"" retrieves those without a catalog; null
	 *        means catalog name should not be used to narrow down the search.
	 * @param schemaPattern - a schema name; must match the schema name as it is
	 *        stored in the database; null means schema name should not be used
	 *        to narrow down the search.
	 * @return a ResultSet object in which each row is a schema description 
	 * @throw SQLException - if a database access error occurs
	 * @since 1.6
	 */
	public ResultSet getSchemas(String catalog, String schemaPattern)
	throws SQLException
	{
		// Needs Testing
		String sql = 
			"SELECT nspname AS TABLE_SCHEM, null AS TABLE_CATALOG " +
			"FROM pg_catalog.pg_namespace " +
			"WHERE nspname !~ '^(pg_toast|pg_temp)' ";

		// vulnerable to injection
		if (schemaPattern != null)
		{
			sql += "AND nspname LIKE '" + schemaPattern + "' ";
			sql += "ESCAPE '" + this.getSearchStringEscape() + "' ";
		}
		sql += "ORDER BY TABLE_SCHEM";

		return createMetaDataStatement().executeQuery(sql);
	}

	/**
	 * Retrieves the catalog names available in this database. The results are
	 * ordered by catalog name
	 *
	 * The catalog column is:
	 *
     * 1. TABLE_CAT String => catalog name

	 * @return a ResultSet object in which each row is a catalog name
	 * @throw SQLException - if a database access error occurs
	 * @since 1.6
	 */
	public java.sql.ResultSet getCatalogs() 
	throws SQLException
	{
		String sql = "SELECT datname AS TABLE_CAT " +
			         "FROM pg_catalog.pg_database " +
			         "ORDER BY TABLE_CAT";
		return createMetaDataStatement().executeQuery(sql);
	}

	/**
	 * Retrieves the table types available in this database. The results are
	 * ordered by table type.
	 *
	 * The table type is:
	 *
	 * 1. TABLE_TYPE String => table type. 
	 * 
	 * Typical types are "TABLE", "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", 
	 * "LOCAL TEMPORARY", "ALIAS", "SYNONYM". 
	 *
	 * @return a ResultSet object in which each row has a single String column
	 *         that is a table type
	 * @throw SQLException - if a database access error occurs
	 * @since 1.6
	 */
	public java.sql.ResultSet getTableTypes() throws SQLException
	{
		String types[] = (String[])s_tableTypeClauses.keySet().toArray(new String[s_tableTypeClauses.size()]);
		sortStringArray(types);

		ResultSetField f[] = new ResultSetField[1];
		ArrayList v = new ArrayList();
		f[0] = new ResultSetField(new String("TABLE_TYPE"), TypeOid.VARCHAR,
			getMaxNameLength());
		for(int i = 0; i < types.length; i++)
		{
			Object[] tuple = new Object[1];
			tuple[0] = types[i];
			v.add(tuple);
		}

		return createSyntheticResultSet(f, v);
	}

	/*
	 * Get a description of table columns available in a catalog. <P>Only
	 * column descriptions matching the catalog, schema, table and column name
	 * criteria are returned. They are ordered by TABLE_SCHEM, TABLE_NAME and
	 * ORDINAL_POSITION. <P>Each column description has the following columns:
	 * <OL> <LI><B>TABLE_CAT</B> String => table catalog (may be null) <LI><B>TABLE_SCHEM</B>
	 * String => table schema (may be null) <LI><B>TABLE_NAME</B> String =>
	 * table name <LI><B>COLUMN_NAME</B> String => column name <LI><B>DATA_TYPE</B>
	 * short => SQL type from java.sql.Types <LI><B>TYPE_NAME</B> String =>
	 * Data source dependent type name <LI><B>COLUMN_SIZE</B> int => column
	 * size. For char or date types this is the maximum number of characters,
	 * for numeric or decimal types this is precision. <LI><B>BUFFER_LENGTH</B>
	 * is not used. <LI><B>DECIMAL_DIGITS</B> int => the number of fractional
	 * digits <LI><B>NUM_PREC_RADIX</B> int => Radix (typically either 10 or
	 * 2) <LI><B>NULLABLE</B> int => is NULL allowed? <UL> <LI> columnNoNulls -
	 * might not allow NULL values <LI> columnNullable - definitely allows NULL
	 * values <LI> columnNullableUnknown - nullability unknown </UL> <LI><B>REMARKS</B>
	 * String => comment describing column (may be null) <LI><B>COLUMN_DEF</B>
	 * String => default value (may be null) <LI><B>SQL_DATA_TYPE</B> int =>
	 * unused <LI><B>SQL_DATETIME_SUB</B> int => unused <LI><B>CHAR_OCTET_LENGTH</B>
	 * int => for char types the maximum number of bytes in the column <LI><B>ORDINAL_POSITION</B>
	 * int => index of column in table (starting at 1) <LI><B>IS_NULLABLE</B>
	 * String => "NO" means column definitely does not allow NULL values; "YES"
	 * means the column might allow NULL values. An empty string means nobody
	 * knows. </OL> @param catalog a catalog name; "" retrieves those without a
	 * catalog @param schemaPattern a schema name pattern; "" retrieves those
	 * without a schema @param tableNamePattern a table name pattern @param
	 * columnNamePattern a column name pattern @return ResultSet each row is a
	 * column description
	 * 
	 * @see #getSearchStringEscape
	 */
	public java.sql.ResultSet getColumns(String catalog, String schemaPattern,
		String tableNamePattern, String columnNamePattern) throws SQLException
	{
		ArrayList v = new ArrayList(); // The new ResultSet tuple stuff
		ResultSetField f[] = new ResultSetField[18]; // The field descriptors
														// for the new ResultSet

		f[0] = new ResultSetField("TABLE_CAT", TypeOid.VARCHAR,
			getMaxNameLength());
		f[1] = new ResultSetField("TABLE_SCHEM", TypeOid.VARCHAR,
			getMaxNameLength());
		f[2] = new ResultSetField("TABLE_NAME", TypeOid.VARCHAR,
			getMaxNameLength());
		f[3] = new ResultSetField("COLUMN_NAME", TypeOid.VARCHAR,
			getMaxNameLength());
		f[4] = new ResultSetField("DATA_TYPE", TypeOid.INT2, 2);
		f[5] = new ResultSetField("TYPE_NAME", TypeOid.VARCHAR,
			getMaxNameLength());
		f[6] = new ResultSetField("COLUMN_SIZE", TypeOid.INT4, 4);
		f[7] = new ResultSetField("BUFFER_LENGTH", TypeOid.VARCHAR,
			getMaxNameLength());
		f[8] = new ResultSetField("DECIMAL_DIGITS", TypeOid.INT4, 4);
		f[9] = new ResultSetField("NUM_PREC_RADIX", TypeOid.INT4, 4);
		f[10] = new ResultSetField("NULLABLE", TypeOid.INT4, 4);
		f[11] = new ResultSetField("REMARKS", TypeOid.VARCHAR,
			getMaxNameLength());
		f[12] = new ResultSetField("COLUMN_DEF", TypeOid.VARCHAR,
			getMaxNameLength());
		f[13] = new ResultSetField("SQL_DATA_TYPE", TypeOid.INT4, 4);
		f[14] = new ResultSetField("SQL_DATETIME_SUB", TypeOid.INT4, 4);
		f[15] = new ResultSetField("CHAR_OCTET_LENGTH", TypeOid.INT4, 4);
		f[16] = new ResultSetField("ORDINAL_POSITION", TypeOid.INT4, 4);
		f[17] = new ResultSetField("IS_NULLABLE", TypeOid.VARCHAR,
			getMaxNameLength());

		String sql = "SELECT n.nspname,c.relname,a.attname,"
				+ " a.atttypid as atttypid,a.attnotnull,a.atttypmod,"
				+ " a.attlen::int4 as attlen,a.attnum,def.adsrc,dsc.description "
				+ " FROM pg_catalog.pg_namespace n "
				+ " JOIN pg_catalog.pg_class c ON (c.relnamespace = n.oid) "
				+ " JOIN pg_catalog.pg_attribute a ON (a.attrelid=c.oid) "
				+ " LEFT JOIN pg_catalog.pg_attrdef def ON (a.attrelid=def.adrelid AND a.attnum = def.adnum) "
				+ " LEFT JOIN pg_catalog.pg_description dsc ON (c.oid=dsc.objoid AND a.attnum = dsc.objsubid) "
				+ " LEFT JOIN pg_catalog.pg_class dc ON (dc.oid=dsc.classoid AND dc.relname='pg_class') "
				+ " LEFT JOIN pg_catalog.pg_namespace dn ON (dc.relnamespace=dn.oid AND dn.nspname='pg_catalog') "
				+ " WHERE a.attnum > 0 AND NOT a.attisdropped "
                + " AND " + resolveSchemaPatternCondition(
                                "n.nspname", schemaPattern);

		if(tableNamePattern != null && !"".equals(tableNamePattern))
		{
			sql += " AND c.relname LIKE '" + escapeQuotes(tableNamePattern)
				+ "' ";
		}
		if(columnNamePattern != null && !"".equals(columnNamePattern))
		{
			sql += " AND a.attname LIKE '" + escapeQuotes(columnNamePattern)
				+ "' ";
		}
		sql += " ORDER BY nspname,relname,attnum ";

		ResultSet rs = m_connection.createStatement().executeQuery(sql);
		while(rs.next())
		{
			Object[] tuple = new Object[18];
			Oid typeOid = (Oid)rs.getObject("atttypid");

			tuple[0] = null; // Catalog name, not supported
			tuple[1] = rs.getString("nspname"); // Schema
			tuple[2] = rs.getString("relname"); // Table name
			tuple[3] = rs.getString("attname"); // Column name
			tuple[4] = new Short((short)m_connection.getSQLType(typeOid));
			String pgType = m_connection.getPGType(typeOid);
			tuple[5] = m_connection.getPGType(typeOid); // Type name

			String defval = rs.getString("adsrc");

			if(defval != null)
			{
				if(pgType.equals("int4"))
				{
					if(defval.indexOf("nextval(") != -1)
						tuple[5] = "serial"; // Type name ==
											 // serial
				}
				else if(pgType.equals("int8"))
				{
					if(defval.indexOf("nextval(") != -1)
						tuple[5] = "bigserial"; // Type name ==
												// bigserial
				}
			}

			// by default no decimal_digits
			// if the type is numeric or decimal we will
			// overwrite later.
			tuple[8] = new Integer(0);

			if(pgType.equals("bpchar") || pgType.equals("varchar"))
			{
				int atttypmod = rs.getInt("atttypmod");
				tuple[6] = new Integer(atttypmod != -1
										? atttypmod - VARHDRSZ
										: 0);
			}
			else if(pgType.equals("numeric") || pgType.equals("decimal"))
			{
				int attypmod = rs.getInt("atttypmod") - VARHDRSZ;
				tuple[6] = new Integer ((attypmod >> 16) & 0xffff);
				tuple[8] = new Integer (attypmod & 0xffff);
				tuple[9] = new Integer(10);
			}
			else if(pgType.equals("bit") || pgType.equals("varbit"))
			{
				tuple[6] = rs.getObject("atttypmod");
				tuple[9] = new Integer(2);
			}
			else
			{
				tuple[6] = rs.getObject("attlen");
				tuple[9] = new Integer(10);
			}

			tuple[7] = null; // Buffer length

			tuple[10] = new Integer(rs
							.getBoolean("attnotnull")
							? java.sql.DatabaseMetaData.columnNoNulls
							: java.sql.DatabaseMetaData.columnNullable); // Nullable
			tuple[11] = rs.getString("description"); // Description (if any)
			tuple[12] = rs.getString("adsrc"); // Column default
			tuple[13] = null; // sql data type (unused)
			tuple[14] = null; // sql datetime sub (unused)
			tuple[15] = tuple[6]; // char octet length
			tuple[16] = new Integer(rs.getInt("attnum")); // ordinal position
			tuple[17] = rs.getBoolean("attnotnull") ? "NO" : "YES"; // Is
																	// nullable
			v.add(tuple);
		}
		rs.close();

		return createSyntheticResultSet(f, v);
	}

	/*
	 * Get a description of the access rights for a table's columns. <P>Only
	 * privileges matching the column name criteria are returned. They are
	 * ordered by COLUMN_NAME and PRIVILEGE. <P>Each privilige description has
	 * the following columns: <OL> <LI><B>TABLE_CAT</B> String => table
	 * catalog (may be null) <LI><B>TABLE_SCHEM</B> String => table schema
	 * (may be null) <LI><B>TABLE_NAME</B> String => table name <LI><B>COLUMN_NAME</B>
	 * String => column name <LI><B>GRANTOR</B> => grantor of access (may be
	 * null) <LI><B>GRANTEE</B> String => grantee of access <LI><B>PRIVILEGE</B>
	 * String => name of access (SELECT, INSERT, UPDATE, REFRENCES, ...) <LI><B>IS_GRANTABLE</B>
	 * String => "YES" if grantee is permitted to grant to others; "NO" if not;
	 * null if unknown </OL> @param catalog a catalog name; "" retrieves those
	 * without a catalog @param schema a schema name; "" retrieves those without
	 * a schema @param table a table name @param columnNamePattern a column name
	 * pattern @return ResultSet each row is a column privilege description
	 * 
	 * @see #getSearchStringEscape
	 */
	public java.sql.ResultSet getColumnPrivileges(String catalog,
		String schema, String table, String columnNamePattern)
	throws SQLException
	{
		ResultSetField f[] = new ResultSetField[8];
		ArrayList v = new ArrayList();

		if(table == null)
			table = "%";

		if(columnNamePattern == null)
			columnNamePattern = "%";

		f[0] = new ResultSetField("TABLE_CAT", TypeOid.VARCHAR,
			getMaxNameLength());
		f[1] = new ResultSetField("TABLE_SCHEM", TypeOid.VARCHAR,
			getMaxNameLength());
		f[2] = new ResultSetField("TABLE_NAME", TypeOid.VARCHAR,
			getMaxNameLength());
		f[3] = new ResultSetField("COLUMN_NAME", TypeOid.VARCHAR,
			getMaxNameLength());
		f[4] = new ResultSetField("GRANTOR", TypeOid.VARCHAR,
			getMaxNameLength());
		f[5] = new ResultSetField("GRANTEE", TypeOid.VARCHAR,
			getMaxNameLength());
		f[6] = new ResultSetField("PRIVILEGE", TypeOid.VARCHAR,
			getMaxNameLength());
		f[7] = new ResultSetField("IS_GRANTABLE", TypeOid.VARCHAR,
			getMaxNameLength());

		String sql = "SELECT n.nspname,c.relname,u.usename,c.relacl,a.attname "
				+ " FROM pg_catalog.pg_namespace n, pg_catalog.pg_class c, pg_catalog.pg_user u, pg_catalog.pg_attribute a "
				+ " WHERE c.relnamespace = n.oid "
				+ " AND u.usesysid = c.relowner " + " AND c.oid = a.attrelid "
				+ " AND c.relkind = 'r' "
				+ " AND a.attnum > 0 AND NOT a.attisdropped "
                + " AND " + resolveSchemaCondition(
                                "n.nspname", schema);

		sql += " AND c.relname = '" + escapeQuotes(table) + "' ";
		if(columnNamePattern != null && !"".equals(columnNamePattern))
		{
			sql += " AND a.attname LIKE '" + escapeQuotes(columnNamePattern)
				+ "' ";
		}
		sql += " ORDER BY attname ";

		ResultSet rs = m_connection.createStatement().executeQuery(sql);
		String schemaName = null;
		String tableName = null;
		String column = null;
		String owner = null;
		String[] acls = null;
		HashMap permissions = null;
		String permNames[] = null;

		while(rs.next())
		{
			schemaName = rs.getString("nspname");
			tableName = rs.getString("relname");
			column = rs.getString("attname");
			owner = rs.getString("usename");
			acls = (String[])rs.getObject("relacl");
			permissions = parseACL(acls, owner);
			permNames = (String[])permissions.keySet().toArray(new String[permissions.size()]);
			sortStringArray(permNames);
			for(int i = 0; i < permNames.length; i++)
			{
				ArrayList grantees = (ArrayList)permissions.get(permNames[i]);
				for(int j = 0; j < grantees.size(); j++)
				{
					String grantee = (String)grantees.get(j);
					String grantable = owner.equals(grantee) ? "YES" : "NO";
					Object[] tuple = new Object[8];
					tuple[0] = null;
					tuple[1] = schemaName;
					tuple[2] = tableName;
					tuple[3] = column;
					tuple[4] = owner;
					tuple[5] = grantee;
					tuple[6] = permNames[i];
					tuple[7] = grantable;
					v.add(tuple);
				}
			}
		}
		rs.close();

		return createSyntheticResultSet(f, v);
	}

	/*
	 * Get a description of the access rights for each table available in a
	 * catalog. This method is currently unimplemented. <P>Only privileges
	 * matching the schema and table name criteria are returned. They are
	 * ordered by TABLE_SCHEM, TABLE_NAME, and PRIVILEGE. <P>Each privilige
	 * description has the following columns: <OL> <LI><B>TABLE_CAT</B> String =>
	 * table catalog (may be null) <LI><B>TABLE_SCHEM</B> String => table
	 * schema (may be null) <LI><B>TABLE_NAME</B> String => table name <LI><B>GRANTOR</B> =>
	 * grantor of access (may be null) <LI><B>GRANTEE</B> String => grantee of
	 * access <LI><B>PRIVILEGE</B> String => name of access (SELECT, INSERT,
	 * UPDATE, REFRENCES, ...) <LI><B>IS_GRANTABLE</B> String => "YES" if
	 * grantee is permitted to grant to others; "NO" if not; null if unknown
	 * </OL> @param catalog a catalog name; "" retrieves those without a catalog
	 * @param schemaPattern a schema name pattern; "" retrieves those without a
	 * schema @param tableNamePattern a table name pattern @return ResultSet
	 * each row is a table privilege description
	 * 
	 * @see #getSearchStringEscape
	 */
	public java.sql.ResultSet getTablePrivileges(String catalog,
		String schemaPattern, String tableNamePattern) throws SQLException
	{
		ResultSetField f[] = new ResultSetField[7];
		ArrayList v = new ArrayList();

		f[0] = new ResultSetField("TABLE_CAT", TypeOid.VARCHAR,
			getMaxNameLength());
		f[1] = new ResultSetField("TABLE_SCHEM", TypeOid.VARCHAR,
			getMaxNameLength());
		f[2] = new ResultSetField("TABLE_NAME", TypeOid.VARCHAR,
			getMaxNameLength());
		f[3] = new ResultSetField("GRANTOR", TypeOid.VARCHAR,
			getMaxNameLength());
		f[4] = new ResultSetField("GRANTEE", TypeOid.VARCHAR,
			getMaxNameLength());
		f[5] = new ResultSetField("PRIVILEGE", TypeOid.VARCHAR,
			getMaxNameLength());
		f[6] = new ResultSetField("IS_GRANTABLE", TypeOid.VARCHAR,
			getMaxNameLength());

		String sql = "SELECT n.nspname,c.relname,u.usename,c.relacl "
				+ " FROM pg_catalog.pg_namespace n, pg_catalog.pg_class c, pg_catalog.pg_user u "
				+ " WHERE c.relnamespace = n.oid "
				+ " AND u.usesysid = c.relowner " + " AND c.relkind = 'r' "
                + " AND " + resolveSchemaPatternCondition(
                                "n.nspname", schemaPattern);

		if(tableNamePattern != null && !"".equals(tableNamePattern))
		{
			sql += " AND c.relname LIKE '" + escapeQuotes(tableNamePattern)
				+ "' ";
		}
		sql += " ORDER BY nspname, relname ";

		ResultSet rs = m_connection.createStatement().executeQuery(sql);
		String schema = null;
		String table = null;
		String owner = null;
		String[] acls = null;
		HashMap permissions = null;
		String permNames[] = null;

		while(rs.next())
		{
			schema = rs.getString("nspname");
			table = rs.getString("relname");
			owner = rs.getString("usename");
			acls = (String[])rs.getObject("relacl");
			permissions = parseACL(acls, owner);
			permNames = (String[])permissions.keySet().toArray(new String[permissions.size()]);
			sortStringArray(permNames);
			for(int i = 0; i < permNames.length; i++)
			{
				ArrayList grantees = (ArrayList)permissions.get(permNames[i]);
				for(int j = 0; j < grantees.size(); j++)
				{
					String grantee = (String)grantees.get(j);
					String grantable = owner.equals(grantee) ? "YES" : "NO";
					Object[] tuple = new Object[7];
					tuple[0] = null;
					tuple[1] = schema;
					tuple[2] = table;
					tuple[3] = owner;
					tuple[4] = grantee;
					tuple[5] = permNames[i];
					tuple[6] = grantable;
					v.add(tuple);
				}
			}
		}
		rs.close();

		return createSyntheticResultSet(f, v);
	}

	private static void sortStringArray(String s[])
	{
		for(int i = 0; i < s.length - 1; i++)
		{
			for(int j = i + 1; j < s.length; j++)
			{
				if(s[i].compareTo(s[j]) > 0)
				{
					String tmp = s[i];
					s[i] = s[j];
					s[j] = tmp;
				}
			}
		}
	}

	/**
	 * Add the user described by the given acl to the ArrayLists of users with the
	 * privileges described by the acl.
	 */
	private void addACLPrivileges(String acl, HashMap privileges)
	{
		int equalIndex = acl.lastIndexOf("=");
		String name = acl.substring(0, equalIndex);
		if(name.length() == 0)
		{
			name = "PUBLIC";
		}
		String privs = acl.substring(equalIndex + 1);
		for(int i = 0; i < privs.length(); i++)
		{
			char c = privs.charAt(i);
			String sqlpriv;
			switch(c)
			{
				case 'a':
					sqlpriv = "INSERT";
					break;
				case 'r':
					sqlpriv = "SELECT";
					break;
				case 'w':
					sqlpriv = "UPDATE";
					break;
				case 'd':
					sqlpriv = "DELETE";
					break;
				case 'R':
					sqlpriv = "RULE";
					break;
				case 'x':
					sqlpriv = "REFERENCES";
					break;
				case 't':
					sqlpriv = "TRIGGER";
					break;
				// the folloowing can't be granted to a table, but
				// we'll keep them for completeness.
				case 'X':
					sqlpriv = "EXECUTE";
					break;
				case 'U':
					sqlpriv = "USAGE";
					break;
				case 'C':
					sqlpriv = "CREATE";
					break;
				case 'T':
					sqlpriv = "CREATE TEMP";
					break;
				default:
					sqlpriv = "UNKNOWN";
			}
			ArrayList usersWithPermission = (ArrayList)privileges.get(sqlpriv);
			if(usersWithPermission == null)
			{
				usersWithPermission = new ArrayList();
				privileges.put(sqlpriv, usersWithPermission);
			}
			usersWithPermission.add(name);
		}
	}

	/**
	 * Take the a String representing an array of ACLs and return a HashMap
	 * mapping the SQL permission name to a ArrayList of usernames who have that
	 * permission.
	 */
	protected HashMap parseACL(String[] aclArray, String owner)
	{
		if(aclArray == null || aclArray.length == 0)
		{
			// null acl is a shortcut for owner having full privs
			aclArray = new String[] { owner + "=arwdRxt" };
		}
		HashMap privileges = new HashMap();
		for(int i = 0; i < aclArray.length; i++)
		{
			String acl = aclArray[i];
			addACLPrivileges(acl, privileges);
		}
		return privileges;
	}

	/**
	 * Retrieves a description of a table's optimal set of columns that uniquely
	 * identifies a row. They are ordered by SCOPE.
	 *
	 * Each column description has the following columns:
     *   1. SCOPE          short => actual scope of result
     *        - bestRowTemporary   : very temporary, while using row
     *        - bestRowTransaction : valid for remainder of current transaction
     *        - bestRowSession     : valid for remainder of current session 
     *   2. COLUMN_NAME   String => column name
     *   3. DATA_TYPE        int => SQL data type from java.sql.Types
     *   4. TYPE_NAME     String => Data source dependent type name, for a UDT the 
	 *          type name is fully qualified
	 *   5. COLUMN_SIZE      int => precision
	 *   6. BUFFER_LENGTH    int => not used
	 *   7. DECIMAL_DIGITS short => scale - Null is returned for data types 
	 *      where DECIMAL_DIGITS is not applicable.
	 *   8. PSEUDO_COLUMN short => is this a pseudo column like an Oracle ROWID
     *        - bestRowUnknown   : may or may not be pseudo column
     *        - bestRowNotPseudo : is NOT a pseudo column
     *        - bestRowPseudo    : is a pseudo column 
	 *
	 * The COLUMN_SIZE column represents the specified column size for the given
	 * column. For numeric data, this is the maximum precision. For character
	 * data, this is the length in characters. For datetime datatypes, this is
	 * the length in characters of the String representation (assuming the
	 * maximum allowed precision of the fractional seconds component). For
	 * binary data, this is the length in bytes. For the ROWID datatype, this is
	 * the length in bytes. Null is returned for data types where the column
	 * size is not applicable.
	 *
	 * @param catalog - a catalog name; must match the catalog name as it is
	 *        stored in the database; "" retrieves those without a catalog; null
	 *        means that the catalog name should not be used to narrow the
	 *        search
	 * @param schema - a schema name; must match the schema name as it is stored
	 *        in the database; "" retrieves those without a schema; null means
	 *        that the schema name should not be used to narrow the search
	 * @param table - a table name; must match the table name as it is stored in
	 *        the database
	 * @param scope - the scope of interest; use same values as SCOPE
	 * @param nullable - include columns that are nullable. 
	 * @return ResultSet - each row is a column description 
	 * @throw SQLException - if a database access error occurs
	 */
	// Implementation note: This is required for Borland's JBuilder to work
	public java.sql.ResultSet getBestRowIdentifier(String catalog,
		String schema, String table, int scope, boolean nullable)
	throws SQLException
	{
		ResultSetField f[] = new ResultSetField[8];
		ArrayList v = new ArrayList(); // The new ResultSet tuple stuff

		f[0] = new ResultSetField("SCOPE", TypeOid.INT2, 2);
		f[1] = new ResultSetField("COLUMN_NAME", TypeOid.VARCHAR,
			getMaxNameLength());
		f[2] = new ResultSetField("DATA_TYPE", TypeOid.INT2, 2);
		f[3] = new ResultSetField("TYPE_NAME", TypeOid.VARCHAR,
			getMaxNameLength());
		f[4] = new ResultSetField("COLUMN_SIZE", TypeOid.INT4, 4);
		f[5] = new ResultSetField("BUFFER_LENGTH", TypeOid.INT4, 4);
		f[6] = new ResultSetField("DECIMAL_DIGITS", TypeOid.INT2, 2);
		f[7] = new ResultSetField("PSEUDO_COLUMN", TypeOid.INT2, 2);

		/*
		 * At the moment this simply returns a table's primary key, if there is
		 * one. I believe other unique indexes, ctid, and oid should also be
		 * considered. -KJ
		 */

		String where = "";
		String from = " FROM pg_catalog.pg_namespace n, pg_catalog.pg_class ct, pg_catalog.pg_class ci, pg_catalog.pg_attribute a, pg_catalog.pg_index i ";
			where = " AND ct.relnamespace = n.oid "
                  + " AND " + resolveSchemaCondition(
                                  "n.nspname", schema);
		String sql = "SELECT a.attname, a.atttypid as atttypid " + from
			+ " WHERE ct.oid=i.indrelid AND ci.oid=i.indexrelid "
			+ " AND a.attrelid=ci.oid AND i.indisprimary "
			+ " AND ct.relname = '" + escapeQuotes(table) + "' " + where
			+ " ORDER BY a.attnum ";

		ResultSet rs = m_connection.createStatement().executeQuery(sql);
		while(rs.next())
		{
			Object[] tuple = new Object[8];
			Oid columnTypeOid = (Oid)rs.getObject("atttypid");
			tuple[0] = new Short((short)scope);
			tuple[1] = rs.getString("attname");
			tuple[2] = new Short((short)m_connection.getSQLType(columnTypeOid));
			tuple[3] = m_connection.getPGType(columnTypeOid);
			tuple[4] = null;
			tuple[5] = null;
			tuple[6] = null;
			tuple[7] = new Short((short)java.sql.DatabaseMetaData.bestRowNotPseudo);
			v.add(tuple);
		}

		return createSyntheticResultSet(f, v);
	}

	/*
	 * Get a description of a table's columns that are automatically updated
	 * when any value in a row is updated. They are unordered. <P>Each column
	 * description has the following columns: <OL> <LI><B>SCOPE</B> short =>
	 * is not used <LI><B>COLUMN_NAME</B> String => column name <LI><B>DATA_TYPE</B>
	 * short => SQL data type from java.sql.Types <LI><B>TYPE_NAME</B> String =>
	 * Data source dependent type name <LI><B>COLUMN_SIZE</B> int => precision
	 * <LI><B>BUFFER_LENGTH</B> int => length of column value in bytes <LI><B>DECIMAL_DIGITS</B>
	 * short => scale <LI><B>PSEUDO_COLUMN</B> short => is this a pseudo
	 * column like an Oracle ROWID <UL> <LI> versionColumnUnknown - may or may
	 * not be pseudo column <LI> versionColumnNotPseudo - is NOT a pseudo column
	 * <LI> versionColumnPseudo - is a pseudo column </UL> </OL> @param catalog
	 * a catalog name; "" retrieves those without a catalog @param schema a
	 * schema name; "" retrieves those without a schema @param table a table
	 * name @return ResultSet each row is a column description
	 */
	public java.sql.ResultSet getVersionColumns(String catalog, String schema,
		String table) throws SQLException
	{
		ResultSetField f[] = new ResultSetField[8];
		ArrayList v = new ArrayList(); // The new ResultSet tuple stuff

		f[0] = new ResultSetField("SCOPE", TypeOid.INT2, 2);
		f[1] = new ResultSetField("COLUMN_NAME", TypeOid.VARCHAR,
			getMaxNameLength());
		f[2] = new ResultSetField("DATA_TYPE", TypeOid.INT2, 2);
		f[3] = new ResultSetField("TYPE_NAME", TypeOid.VARCHAR,
			getMaxNameLength());
		f[4] = new ResultSetField("COLUMN_SIZE", TypeOid.INT4, 4);
		f[5] = new ResultSetField("BUFFER_LENGTH", TypeOid.INT4, 4);
		f[6] = new ResultSetField("DECIMAL_DIGITS", TypeOid.INT2, 2);
		f[7] = new ResultSetField("PSEUDO_COLUMN", TypeOid.INT2, 2);

		Object[] tuple = new Object[8];

		/*
		 * Postgresql does not have any column types that are automatically
		 * updated like some databases' timestamp type. We can't tell what rules
		 * or triggers might be doing, so we are left with the system columns
		 * that change on an update. An update may change all of the following
		 * system columns: ctid, xmax, xmin, cmax, and cmin. Depending on if we
		 * are in a transaction and wether we roll it back or not the only
		 * guaranteed change is to ctid. -KJ
		 */

		tuple[0] = null;
		tuple[1] = "ctid";
		tuple[2] = new Short((short)m_connection.getSQLType("tid"));
		tuple[3] = "tid";
		tuple[4] = null;
		tuple[5] = null;
		tuple[6] = null;
		tuple[7] = new Short((short)java.sql.DatabaseMetaData.versionColumnPseudo);
		v.add(tuple);

		/*
		 * Perhaps we should check that the given catalog.schema.table actually
		 * exists. -KJ
		 */
		return createSyntheticResultSet(f, v);
	}

	/*
	 * Get a description of a table's primary key columns. They are ordered by
	 * COLUMN_NAME. <P>Each column description has the following columns: <OL>
	 * <LI><B>TABLE_CAT</B> String => table catalog (may be null) <LI><B>TABLE_SCHEM</B>
	 * String => table schema (may be null) <LI><B>TABLE_NAME</B> String =>
	 * table name <LI><B>COLUMN_NAME</B> String => column name <LI><B>KEY_SEQ</B>
	 * short => sequence number within primary key <LI><B>PK_NAME</B> String =>
	 * primary key name (may be null) </OL> @param catalog a catalog name; ""
	 * retrieves those without a catalog @param schema a schema name pattern; ""
	 * retrieves those without a schema @param table a table name @return
	 * ResultSet each row is a primary key column description
	 */
	public java.sql.ResultSet getPrimaryKeys(String catalog, String schema,
		String table) throws SQLException
	{
		String from;
		String where = "";
		String select = "SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM, ";
			from = " FROM pg_catalog.pg_namespace n, pg_catalog.pg_class ct, pg_catalog.pg_class ci, pg_catalog.pg_attribute a, pg_catalog.pg_index i ";
			where = " AND ct.relnamespace = n.oid AND " +
                resolveSchemaCondition("n.nspname", schema);

		String sql = select + " ct.relname AS TABLE_NAME, "
			+ " a.attname AS COLUMN_NAME, " + " a.attnum::int2 AS KEY_SEQ, "
			+ " ci.relname AS PK_NAME " + from
			+ " WHERE ct.oid=i.indrelid AND ci.oid=i.indexrelid "
			+ " AND a.attrelid=ci.oid AND i.indisprimary ";
		if(table != null && !"".equals(table))
		{
			sql += " AND ct.relname = '" + escapeQuotes(table) + "' ";
		}
		sql += where + " ORDER BY table_name, pk_name, key_seq";

        return createMetaDataStatement().executeQuery(sql);
	}

	/**
	 * @param primaryCatalog
	 * @param primarySchema
	 * @param primaryTable if provided will get the keys exported by this table
	 * @param foreignTable if provided will get the keys imported by this table
	 * @return ResultSet
	 * @throws SQLException
	 */

	protected java.sql.ResultSet getImportedExportedKeys(String primaryCatalog,
		String primarySchema, String primaryTable, String foreignCatalog,
		String foreignSchema, String foreignTable) throws SQLException
	{
		ResultSetField f[] = new ResultSetField[14];

		f[0] = new ResultSetField("PKTABLE_CAT", TypeOid.VARCHAR,
			getMaxNameLength());
		f[1] = new ResultSetField("PKTABLE_SCHEM", TypeOid.VARCHAR,
			getMaxNameLength());
		f[2] = new ResultSetField("PKTABLE_NAME", TypeOid.VARCHAR,
			getMaxNameLength());
		f[3] = new ResultSetField("PKCOLUMN_NAME", TypeOid.VARCHAR,
			getMaxNameLength());
		f[4] = new ResultSetField("FKTABLE_CAT", TypeOid.VARCHAR,
			getMaxNameLength());
		f[5] = new ResultSetField("FKTABLE_SCHEM", TypeOid.VARCHAR,
			getMaxNameLength());
		f[6] = new ResultSetField("FKTABLE_NAME", TypeOid.VARCHAR,
			getMaxNameLength());
		f[7] = new ResultSetField("FKCOLUMN_NAME", TypeOid.VARCHAR,
			getMaxNameLength());
		f[8] = new ResultSetField("KEY_SEQ", TypeOid.INT2, 2);
		f[9] = new ResultSetField("UPDATE_RULE", TypeOid.INT2, 2);
		f[10] = new ResultSetField("DELETE_RULE", TypeOid.INT2, 2);
		f[11] = new ResultSetField("FK_NAME", TypeOid.VARCHAR,
			getMaxNameLength());
		f[12] = new ResultSetField("PK_NAME", TypeOid.VARCHAR,
			getMaxNameLength());
		f[13] = new ResultSetField("DEFERRABILITY", TypeOid.INT2, 2);

		/*
		 * The addition of the pg_constraint in 7.3 table should have really
		 * helped us out here, but it comes up just a bit short. - The conkey,
		 * confkey columns aren't really useful without contrib/array unless we
		 * want to issues separate queries. - Unique indexes that can support
		 * foreign keys are not necessarily added to pg_constraint. Also
		 * multiple unique indexes covering the same keys can be created which
		 * make it difficult to determine the PK_NAME field.
		 */
		String sql = "SELECT NULL::text AS PKTABLE_CAT, pkn.nspname AS PKTABLE_SCHEM, pkc.relname AS PKTABLE_NAME, pka.attname AS PKCOLUMN_NAME, "
			+ "NULL::text AS FKTABLE_CAT, fkn.nspname AS FKTABLE_SCHEM, fkc.relname AS FKTABLE_NAME, fka.attname AS FKCOLUMN_NAME, "
			+ "pos.n::int2 AS KEY_SEQ, "
			+ "CASE con.confupdtype "
			+ " WHEN 'c' THEN "
			+ DatabaseMetaData.importedKeyCascade
			+ " WHEN 'n' THEN "
			+ DatabaseMetaData.importedKeySetNull
			+ " WHEN 'd' THEN "
			+ DatabaseMetaData.importedKeySetDefault
			+ " WHEN 'r' THEN "
			+ DatabaseMetaData.importedKeyRestrict
			+ " WHEN 'a' THEN "
			+ DatabaseMetaData.importedKeyNoAction
			+ " ELSE NULL END::int2 AS UPDATE_RULE, "
			+ "CASE con.confdeltype "
			+ " WHEN 'c' THEN "
			+ DatabaseMetaData.importedKeyCascade
			+ " WHEN 'n' THEN "
			+ DatabaseMetaData.importedKeySetNull
			+ " WHEN 'd' THEN "
			+ DatabaseMetaData.importedKeySetDefault
			+ " WHEN 'r' THEN "
			+ DatabaseMetaData.importedKeyRestrict
			+ " WHEN 'a' THEN "
			+ DatabaseMetaData.importedKeyNoAction
			+ " ELSE NULL END::int2 AS DELETE_RULE, "
			+ "con.conname AS FK_NAME, pkic.relname AS PK_NAME, "
			+ "CASE "
			+ " WHEN con.condeferrable AND con.condeferred THEN "
			+ DatabaseMetaData.importedKeyInitiallyDeferred
			+ " WHEN con.condeferrable THEN "
			+ DatabaseMetaData.importedKeyInitiallyImmediate
			+ " ELSE "
			+ DatabaseMetaData.importedKeyNotDeferrable
			+ " END::int2 AS DEFERRABILITY "
			+ " FROM "
			+ " pg_catalog.pg_namespace pkn, pg_catalog.pg_class pkc, pg_catalog.pg_attribute pka, "
			+ " pg_catalog.pg_namespace fkn, pg_catalog.pg_class fkc, pg_catalog.pg_attribute fka, "
			+ " pg_catalog.pg_constraint con, "
			+ " pg_catalog.generate_series(1, " + getMaxIndexKeys() + ") pos(n), "
			+ " pg_catalog.pg_depend dep, pg_catalog.pg_class pkic "
			+ " WHERE pkn.oid = pkc.relnamespace AND pkc.oid = pka.attrelid AND pka.attnum = con.confkey[pos.n] AND con.confrelid = pkc.oid "
			+ " AND fkn.oid = fkc.relnamespace AND fkc.oid = fka.attrelid AND fka.attnum = con.conkey[pos.n] AND con.conrelid = fkc.oid "
			+ " AND con.contype = 'f' AND con.oid = dep.objid AND pkic.oid = dep.refobjid AND pkic.relkind = 'i' AND dep.classid = 'pg_constraint'::regclass::oid AND dep.refclassid = 'pg_class'::regclass::oid " +
             " AND " + resolveSchemaCondition("pkn.nspname", primarySchema) +
             " AND " + resolveSchemaCondition("fkn.nspname", foreignSchema);

        if(primaryTable != null && !"".equals(primaryTable))
		{
			sql += " AND pkc.relname = '" + escapeQuotes(primaryTable)
				+ "' ";
		}
		if(foreignTable != null && !"".equals(foreignTable))
		{
			sql += " AND fkc.relname = '" + escapeQuotes(foreignTable)
				+ "' ";
		}

		if(primaryTable != null)
		{
			sql += " ORDER BY fkn.nspname,fkc.relname,pos.n";
		}
		else
		{
			sql += " ORDER BY pkn.nspname,pkc.relname,pos.n";
		}

		return createMetaDataStatement().executeQuery(sql);
	}

	/*
	 * Get a description of the primary key columns that are referenced by a
	 * table's foreign key columns (the primary keys imported by a table). They
	 * are ordered by PKTABLE_CAT, PKTABLE_SCHEM, PKTABLE_NAME, and KEY_SEQ. <P>Each
	 * primary key column description has the following columns: <OL> <LI><B>PKTABLE_CAT</B>
	 * String => primary key table catalog being imported (may be null) <LI><B>PKTABLE_SCHEM</B>
	 * String => primary key table schema being imported (may be null) <LI><B>PKTABLE_NAME</B>
	 * String => primary key table name being imported <LI><B>PKCOLUMN_NAME</B>
	 * String => primary key column name being imported <LI><B>FKTABLE_CAT</B>
	 * String => foreign key table catalog (may be null) <LI><B>FKTABLE_SCHEM</B>
	 * String => foreign key table schema (may be null) <LI><B>FKTABLE_NAME</B>
	 * String => foreign key table name <LI><B>FKCOLUMN_NAME</B> String =>
	 * foreign key column name <LI><B>KEY_SEQ</B> short => sequence number
	 * within foreign key <LI><B>UPDATE_RULE</B> short => What happens to
	 * foreign key when primary is updated: <UL> <LI> importedKeyCascade -
	 * change imported key to agree with primary key update <LI>
	 * importedKeyRestrict - do not allow update of primary key if it has been
	 * imported <LI> importedKeySetNull - change imported key to NULL if its
	 * primary key has been updated </UL> <LI><B>DELETE_RULE</B> short => What
	 * happens to the foreign key when primary is deleted. <UL> <LI>
	 * importedKeyCascade - delete rows that import a deleted key <LI>
	 * importedKeyRestrict - do not allow delete of primary key if it has been
	 * imported <LI> importedKeySetNull - change imported key to NULL if its
	 * primary key has been deleted </UL> <LI><B>FK_NAME</B> String => foreign
	 * key name (may be null) <LI><B>PK_NAME</B> String => primary key name
	 * (may be null) </OL> @param catalog a catalog name; "" retrieves those
	 * without a catalog @param schema a schema name pattern; "" retrieves those
	 * without a schema @param table a table name @return ResultSet each row is
	 * a primary key column description
	 * 
	 * @see #getExportedKeys
	 */
	public java.sql.ResultSet getImportedKeys(String catalog, String schema,
		String table) throws SQLException
	{
		return getImportedExportedKeys(null, null, null, catalog, schema, table);
	}

	/*
	 * Get a description of a foreign key columns that reference a table's
	 * primary key columns (the foreign keys exported by a table). They are
	 * ordered by FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, and KEY_SEQ. This
	 * method is currently unimplemented. <P>Each foreign key column
	 * description has the following columns: <OL> <LI><B>PKTABLE_CAT</B>
	 * String => primary key table catalog (may be null) <LI><B>PKTABLE_SCHEM</B>
	 * String => primary key table schema (may be null) <LI><B>PKTABLE_NAME</B>
	 * String => primary key table name <LI><B>PKCOLUMN_NAME</B> String =>
	 * primary key column name <LI><B>FKTABLE_CAT</B> String => foreign key
	 * table catalog (may be null) being exported (may be null) <LI><B>FKTABLE_SCHEM</B>
	 * String => foreign key table schema (may be null) being exported (may be
	 * null) <LI><B>FKTABLE_NAME</B> String => foreign key table name being
	 * exported <LI><B>FKCOLUMN_NAME</B> String => foreign key column name
	 * being exported <LI><B>KEY_SEQ</B> short => sequence number within
	 * foreign key <LI><B>UPDATE_RULE</B> short => What happens to foreign key
	 * when primary is updated: <UL> <LI> importedKeyCascade - change imported
	 * key to agree with primary key update <LI> importedKeyRestrict - do not
	 * allow update of primary key if it has been imported <LI>
	 * importedKeySetNull - change imported key to NULL if its primary key has
	 * been updated </UL> <LI><B>DELETE_RULE</B> short => What happens to the
	 * foreign key when primary is deleted. <UL> <LI> importedKeyCascade -
	 * delete rows that import a deleted key <LI> importedKeyRestrict - do not
	 * allow delete of primary key if it has been imported <LI>
	 * importedKeySetNull - change imported key to NULL if its primary key has
	 * been deleted </UL> <LI><B>FK_NAME</B> String => foreign key identifier
	 * (may be null) <LI><B>PK_NAME</B> String => primary key identifier (may
	 * be null) </OL> @param catalog a catalog name; "" retrieves those without
	 * a catalog @param schema a schema name pattern; "" retrieves those without
	 * a schema @param table a table name @return ResultSet each row is a
	 * foreign key column description
	 * 
	 * @see #getImportedKeys
	 */
	public java.sql.ResultSet getExportedKeys(String catalog, String schema,
		String table) throws SQLException
	{
		return getImportedExportedKeys(catalog, schema, table, null, null, null);
	}

	/*
	 * Get a description of the foreign key columns in the foreign key table
	 * that reference the primary key columns of the primary key table (describe
	 * how one table imports another's key.) This should normally return a
	 * single foreign key/primary key pair (most tables only import a foreign
	 * key from a table once.) They are ordered by FKTABLE_CAT, FKTABLE_SCHEM,
	 * FKTABLE_NAME, and KEY_SEQ. This method is currently unimplemented. <P>Each
	 * foreign key column description has the following columns: <OL> <LI><B>PKTABLE_CAT</B>
	 * String => primary key table catalog (may be null) <LI><B>PKTABLE_SCHEM</B>
	 * String => primary key table schema (may be null) <LI><B>PKTABLE_NAME</B>
	 * String => primary key table name <LI><B>PKCOLUMN_NAME</B> String =>
	 * primary key column name <LI><B>FKTABLE_CAT</B> String => foreign key
	 * table catalog (may be null) being exported (may be null) <LI><B>FKTABLE_SCHEM</B>
	 * String => foreign key table schema (may be null) being exported (may be
	 * null) <LI><B>FKTABLE_NAME</B> String => foreign key table name being
	 * exported <LI><B>FKCOLUMN_NAME</B> String => foreign key column name
	 * being exported <LI><B>KEY_SEQ</B> short => sequence number within
	 * foreign key <LI><B>UPDATE_RULE</B> short => What happens to foreign key
	 * when primary is updated: <UL> <LI> importedKeyCascade - change imported
	 * key to agree with primary key update <LI> importedKeyRestrict - do not
	 * allow update of primary key if it has been imported <LI>
	 * importedKeySetNull - change imported key to NULL if its primary key has
	 * been updated </UL> <LI><B>DELETE_RULE</B> short => What happens to the
	 * foreign key when primary is deleted. <UL> <LI> importedKeyCascade -
	 * delete rows that import a deleted key <LI> importedKeyRestrict - do not
	 * allow delete of primary key if it has been imported <LI>
	 * importedKeySetNull - change imported key to NULL if its primary key has
	 * been deleted </UL> <LI><B>FK_NAME</B> String => foreign key identifier
	 * (may be null) <LI><B>PK_NAME</B> String => primary key identifier (may
	 * be null) </OL> @param catalog a catalog name; "" retrieves those without
	 * a catalog @param schema a schema name pattern; "" retrieves those without
	 * a schema @param table a table name @return ResultSet each row is a
	 * foreign key column description
	 * 
	 * @see #getImportedKeys
	 */
	public java.sql.ResultSet getCrossReference(String primaryCatalog,
		String primarySchema, String primaryTable, String foreignCatalog,
		String foreignSchema, String foreignTable) throws SQLException
	{
		return getImportedExportedKeys(primaryCatalog, primarySchema,
			primaryTable, foreignCatalog, foreignSchema, foreignTable);
	}

	/*
	 * Get a description of all the standard SQL types supported by this
	 * database. They are ordered by DATA_TYPE and then by how closely the data
	 * type maps to the corresponding JDBC SQL type. <P>Each type description
	 * has the following columns: <OL> <LI><B>TYPE_NAME</B> String => Type
	 * name <LI><B>DATA_TYPE</B> short => SQL data type from java.sql.Types
	 * <LI><B>PRECISION</B> int => maximum precision <LI><B>LITERAL_PREFIX</B>
	 * String => prefix used to quote a literal (may be null) <LI><B>LITERAL_SUFFIX</B>
	 * String => suffix used to quote a literal (may be null) <LI><B>CREATE_PARAMS</B>
	 * String => parameters used in creating the type (may be null) <LI><B>NULLABLE</B>
	 * short => can you use NULL for this type? <UL> <LI> typeNoNulls - does not
	 * allow NULL values <LI> typeNullable - allows NULL values <LI>
	 * typeNullableUnknown - nullability unknown </UL> <LI><B>CASE_SENSITIVE</B>
	 * boolean=> is it case sensitive? <LI><B>SEARCHABLE</B> short => can you
	 * use "WHERE" based on this type: <UL> <LI> typePredNone - No support <LI>
	 * typePredChar - Only supported with WHERE .. LIKE <LI> typePredBasic -
	 * Supported except for WHERE .. LIKE <LI> typeSearchable - Supported for
	 * all WHERE .. </UL> <LI><B>UNSIGNED_ATTRIBUTE</B> boolean => is it
	 * unsigned? <LI><B>FIXED_PREC_SCALE</B> boolean => can it be a money
	 * value? <LI><B>AUTO_INCREMENT</B> boolean => can it be used for an
	 * auto-increment value? <LI><B>LOCAL_TYPE_NAME</B> String => localized
	 * version of type name (may be null) <LI><B>MINIMUM_SCALE</B> short =>
	 * minimum scale supported <LI><B>MAXIMUM_SCALE</B> short => maximum scale
	 * supported <LI><B>SQL_DATA_TYPE</B> int => unused <LI><B>SQL_DATETIME_SUB</B>
	 * int => unused <LI><B>NUM_PREC_RADIX</B> int => usually 2 or 10 </OL>
	 * @return ResultSet each row is a SQL type description
	 */
	public java.sql.ResultSet getTypeInfo() throws SQLException
	{

		ResultSetField f[] = new ResultSetField[18];
		ArrayList v = new ArrayList(); // The new ResultSet tuple stuff

		f[0] = new ResultSetField("TYPE_NAME", TypeOid.VARCHAR,
			getMaxNameLength());
		f[1] = new ResultSetField("DATA_TYPE", TypeOid.INT2, 2);
		f[2] = new ResultSetField("PRECISION", TypeOid.INT4, 4);
		f[3] = new ResultSetField("LITERAL_PREFIX", TypeOid.VARCHAR,
			getMaxNameLength());
		f[4] = new ResultSetField("LITERAL_SUFFIX", TypeOid.VARCHAR,
			getMaxNameLength());
		f[5] = new ResultSetField("CREATE_PARAMS", TypeOid.VARCHAR,
			getMaxNameLength());
		f[6] = new ResultSetField("NULLABLE", TypeOid.INT2, 2);
		f[7] = new ResultSetField("CASE_SENSITIVE", TypeOid.BOOL, 1);
		f[8] = new ResultSetField("SEARCHABLE", TypeOid.INT2, 2);
		f[9] = new ResultSetField("UNSIGNED_ATTRIBUTE", TypeOid.BOOL, 1);
		f[10] = new ResultSetField("FIXED_PREC_SCALE", TypeOid.BOOL, 1);
		f[11] = new ResultSetField("AUTO_INCREMENT", TypeOid.BOOL, 1);
		f[12] = new ResultSetField("LOCAL_TYPE_NAME", TypeOid.VARCHAR,
			getMaxNameLength());
		f[13] = new ResultSetField("MINIMUM_SCALE", TypeOid.INT2, 2);
		f[14] = new ResultSetField("MAXIMUM_SCALE", TypeOid.INT2, 2);
		f[15] = new ResultSetField("SQL_DATA_TYPE", TypeOid.INT4, 4);
		f[16] = new ResultSetField("SQL_DATETIME_SUB", TypeOid.INT4, 4);
		f[17] = new ResultSetField("NUM_PREC_RADIX", TypeOid.INT4, 4);

		String sql = "SELECT typname FROM pg_catalog.pg_type where typrelid = 0";

		ResultSet rs = m_connection.createStatement().executeQuery(sql);
		// cache some results, this will keep memory useage down, and speed
		// things up a little.
		Integer i9 = new Integer(9);
		Integer i10 = new Integer(10);
		Short nn = new Short((short)java.sql.DatabaseMetaData.typeNoNulls);
		Short ts = new Short((short)java.sql.DatabaseMetaData.typeSearchable);

		String typname = null;

		while(rs.next())
		{
			Object[] tuple = new Object[18];
			typname = rs.getString(1);
			tuple[0] = typname;
			tuple[1] = new Short((short)m_connection.getSQLType(typname));
			tuple[2] = i9; // for now
			tuple[6] = nn; // for now
			tuple[7] = Boolean.FALSE; // false for now - not case sensitive
			tuple[8] = ts;
			tuple[9] = Boolean.FALSE; // false for now - it's signed
			tuple[10] = Boolean.FALSE; // false for now - must handle money
			tuple[11] = Boolean.FALSE; // false - it isn't autoincrement

			// 12 - LOCAL_TYPE_NAME is null
			// 13 & 14 ?
			// 15 & 16 are unused so we return null
			tuple[17] = i10; // everything is base 10
			v.add(tuple);

			// add pseudo-type serial, bigserial
			if(typname.equals("int4"))
			{
				Object[] tuple1 = (Object[])tuple.clone();

				tuple1[0] = "serial";
				tuple1[11] = Boolean.TRUE;
				v.add(tuple1);
			}
			else if(typname.equals("int8"))
			{
				Object[] tuple1 = (Object[])tuple.clone();

				tuple1[0] = "bigserial";
				tuple1[11] = Boolean.TRUE;
				v.add(tuple1);
			}

		}
		rs.close();

		return createSyntheticResultSet(f, v);
	}

	/*
	 * Get a description of a table's indices and statistics. They are ordered
	 * by NON_UNIQUE, TYPE, INDEX_NAME, and ORDINAL_POSITION. <P>Each index
	 * column description has the following columns: <OL> <LI><B>TABLE_CAT</B>
	 * String => table catalog (may be null) <LI><B>TABLE_SCHEM</B> String =>
	 * table schema (may be null) <LI><B>TABLE_NAME</B> String => table name
	 * <LI><B>NON_UNIQUE</B> boolean => Can index values be non-unique? false
	 * when TYPE is tableIndexStatistic <LI><B>INDEX_QUALIFIER</B> String =>
	 * index catalog (may be null); null when TYPE is tableIndexStatistic <LI><B>INDEX_NAME</B>
	 * String => index name; null when TYPE is tableIndexStatistic <LI><B>TYPE</B>
	 * short => index type: <UL> <LI> tableIndexStatistic - this identifies
	 * table statistics that are returned in conjuction with a table's index
	 * descriptions <LI> tableIndexClustered - this is a clustered index <LI>
	 * tableIndexHashed - this is a hashed index <LI> tableIndexOther - this is
	 * some other style of index </UL> <LI><B>ORDINAL_POSITION</B> short =>
	 * column sequence number within index; zero when TYPE is
	 * tableIndexStatistic <LI><B>COLUMN_NAME</B> String => column name; null
	 * when TYPE is tableIndexStatistic <LI><B>ASC_OR_DESC</B> String =>
	 * column sort sequence, "A" => ascending "D" => descending, may be null if
	 * sort sequence is not supported; null when TYPE is tableIndexStatistic
	 * <LI><B>CARDINALITY</B> int => When TYPE is tableIndexStatisic then this
	 * is the number of rows in the table; otherwise it is the number of unique
	 * values in the index. <LI><B>PAGES</B> int => When TYPE is
	 * tableIndexStatisic then this is the number of pages used for the table,
	 * otherwise it is the number of pages used for the current index. <LI><B>FILTER_CONDITION</B>
	 * String => Filter condition, if any. (may be null) </OL> @param catalog a
	 * catalog name; "" retrieves those without a catalog @param schema a schema
	 * name pattern; "" retrieves those without a schema @param table a table
	 * name @param unique when true, return only indices for unique values; when
	 * false, return indices regardless of whether unique or not @param
	 * approximate when true, result is allowed to reflect approximate or out of
	 * data values; when false, results are requested to be accurate @return
	 * ResultSet each row is an index column description
	 */
	// Implementation note: This is required for Borland's JBuilder to work
	public java.sql.ResultSet getIndexInfo(String catalog, String schema,
		String tableName, boolean unique, boolean approximate)
	throws SQLException
	{
		String select = "SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM, ";
		String from = " FROM pg_catalog.pg_namespace n, pg_catalog.pg_class ct, pg_catalog.pg_class ci, pg_catalog.pg_index i, pg_catalog.pg_attribute a, pg_catalog.pg_am am ";
		String where =
            " AND n.oid = ct.relnamespace " +
            " AND " + resolveSchemaCondition("n.nspname", schema);

		String sql = select
			+ " ct.relname AS TABLE_NAME, NOT i.indisunique AS NON_UNIQUE, NULL AS INDEX_QUALIFIER, ci.relname AS INDEX_NAME, "
			+ " CASE i.indisclustered "
			+ " WHEN true THEN "
			+ java.sql.DatabaseMetaData.tableIndexClustered
			+ " ELSE CASE am.amname "
			+ " WHEN 'hash' THEN "
			+ java.sql.DatabaseMetaData.tableIndexHashed
			+ " ELSE "
			+ java.sql.DatabaseMetaData.tableIndexOther
			+ " END "
			+ " END::int2 AS TYPE, "
			+ " a.attnum::int2 AS ORDINAL_POSITION, "
			+ " a.attname AS COLUMN_NAME, "
			+ " NULL AS ASC_OR_DESC, "
			+ " ci.reltuples AS CARDINALITY, "
			+ " ci.relpages AS PAGES, "
			+ " NULL AS FILTER_CONDITION "
			+ from
			+ " WHERE ct.oid=i.indrelid AND ci.oid=i.indexrelid AND a.attrelid=ci.oid AND ci.relam=am.oid "
			+ where + " AND ct.relname = '" + escapeQuotes(tableName) + "' ";

		if(unique)
		{
			sql += " AND i.indisunique ";
		}
		sql += " ORDER BY NON_UNIQUE, TYPE, INDEX_NAME, ORDINAL_POSITION ";
		return createMetaDataStatement().executeQuery(sql);
	}

	// ** JDBC 2 Extensions **

	/*
	 * Does the database support the given result set type? @param type -
	 * defined in java.sql.ResultSet @return true if so; false otherwise
	 * @exception SQLException - if a database access error occurs
	 */
	public boolean supportsResultSetType(int type) throws SQLException
	{
		// The only type we support
		return type == java.sql.ResultSet.TYPE_FORWARD_ONLY;
	}

	/*
	 * Does the database support the concurrency type in combination with the
	 * given result set type? @param type - defined in java.sql.ResultSet @param
	 * concurrency - type defined in java.sql.ResultSet @return true if so;
	 * false otherwise @exception SQLException - if a database access error
	 * occurs
	 */
	public boolean supportsResultSetConcurrency(int type, int concurrency)
	throws SQLException
	{
		// These combinations are not supported!
		if(type != java.sql.ResultSet.TYPE_FORWARD_ONLY)
			return false;

		// We support only Concur Read Only
		if(concurrency != java.sql.ResultSet.CONCUR_READ_ONLY)
			return false;

		// Everything else we do
		return true;
	}

	/* lots of unsupported stuff... */
	public boolean ownUpdatesAreVisible(int type) throws SQLException
	{
		return true;
	}

	public boolean ownDeletesAreVisible(int type) throws SQLException
	{
		return true;
	}

	public boolean ownInsertsAreVisible(int type) throws SQLException
	{
		// indicates that
		return true;
	}

	public boolean othersUpdatesAreVisible(int type) throws SQLException
	{
		return false;
	}

	public boolean othersDeletesAreVisible(int i) throws SQLException
	{
		return false;
	}

	public boolean othersInsertsAreVisible(int type) throws SQLException
	{
		return false;
	}

	public boolean updatesAreDetected(int type) throws SQLException
	{
		return false;
	}

	public boolean deletesAreDetected(int i) throws SQLException
	{
		return false;
	}

	public boolean insertsAreDetected(int type) throws SQLException
	{
		return false;
	}

	/*
	 * Indicates whether the driver supports batch updates.
	 */
	public boolean supportsBatchUpdates() throws SQLException
	{
		return true;
	}

	/**
	 * @param catalog String
	 * @param schemaPattern String
	 * @param typeNamePattern String
	 * @param types int[]
	 * @throws SQLException
	 * @return ResultSet
	 */
	public java.sql.ResultSet getUDTs(String catalog, String schemaPattern,
		String typeNamePattern, int[] types) throws SQLException
	{
		String sql = "select "
			+ "null as type_cat, n.nspname as type_schem, t.typname as type_name,  null as class_name, "
			+ "CASE WHEN t.typtype='c' then "
			+ java.sql.Types.STRUCT
			+ " else "
			+ java.sql.Types.DISTINCT
			+ " end as data_type, pg_catalog.obj_description(t.oid, 'pg_type')  "
			+ "as remarks, CASE WHEN t.typtype = 'd' then  (select CASE";

		for(int i = 0; i < SPIConnection.JDBC3_TYPE_NAMES.length; i++)
		{
			sql += " when typname = '" + SPIConnection.JDBC_TYPE_NUMBERS[i]
				+ "' then " + SPIConnection.JDBC_TYPE_NUMBERS[i];
		}

		sql += " else "
			+ java.sql.Types.OTHER
			+ " end from pg_type where oid=t.typbasetype) "
			+ "else null end as base_type "
			+ "from pg_catalog.pg_type t, pg_catalog.pg_namespace n where t.typnamespace = n.oid and n.nspname != 'pg_catalog' and n.nspname != 'pg_toast'";

		String toAdd = "";
		if(types != null)
		{
			toAdd += " and (false ";
			for(int i = 0; i < types.length; i++)
			{
				switch(types[i])
				{
					case java.sql.Types.STRUCT:
						toAdd += " or t.typtype = 'c'";
						break;
					case java.sql.Types.DISTINCT:
						toAdd += " or t.typtype = 'd'";
						break;
				}
			}
			toAdd += " ) ";
		}
		else
		{
			toAdd += " and t.typtype IN ('c','d') ";
		}
		// spec says that if typeNamePattern is a fully qualified name
		// then the schema and catalog are ignored

		if(typeNamePattern != null)
		{
			// search for qualifier
			int firstQualifier = typeNamePattern.indexOf('.');
			int secondQualifier = typeNamePattern.lastIndexOf('.');

			if(firstQualifier != -1) // if one of them is -1 they both will
										// be
			{
				if(firstQualifier != secondQualifier)
				{
					// we have a catalog.schema.typename, ignore catalog
					schemaPattern = typeNamePattern.substring(
						firstQualifier + 1, secondQualifier);
				}
				else
				{
					// we just have a schema.typename
					schemaPattern = typeNamePattern
						.substring(0, firstQualifier);
				}
				// strip out just the typeName
				typeNamePattern = typeNamePattern
					.substring(secondQualifier + 1);
			}
			toAdd += " and t.typname like '" + escapeQuotes(typeNamePattern)
				+ "'";
		}

		// schemaPattern may have been modified above
		if(schemaPattern != null)
		{
			toAdd += " and n.nspname like '" + escapeQuotes(schemaPattern)
				+ "'";
		}
		sql += toAdd;
		sql += " order by data_type, type_schem, type_name";
		java.sql.ResultSet rs = createMetaDataStatement().executeQuery(sql);

		return rs;
	}

	/*
	 * Retrieves the connection that produced this metadata object. @return the
	 * connection that produced this metadata object
	 */
	public Connection getConnection() throws SQLException
	{
		return m_connection;
	}

	/* I don't find these in the spec!?! */

	public boolean rowChangesAreDetected(int type) throws SQLException
	{
		return false;
	}

	public boolean rowChangesAreVisible(int type) throws SQLException
	{
		return false;
	}

	private Statement createMetaDataStatement() throws SQLException
	{
		return m_connection.createStatement(
            java.sql.ResultSet.TYPE_FORWARD_ONLY,
			java.sql.ResultSet.CONCUR_READ_ONLY);
	}

	/**
	 * Retrieves whether this database supports savepoints.
	 * 
	 * @return <code>true</code> if savepoints are supported;
	 *         <code>false</code> otherwise
	 * @exception SQLException if a database access error occurs
	 * @since 1.4
	 */
	public boolean supportsSavepoints() throws SQLException
	{
		return this.getDatabaseMajorVersion() >= 8;
	}

	/**
	 * Retrieves whether this database supports named parameters to callable
	 * statements.
	 * 
	 * @return <code>true</code> if named parameters are supported;
	 *         <code>false</code> otherwise
	 * @exception SQLException if a database access error occurs
	 * @since 1.4
	 */
	public boolean supportsNamedParameters() throws SQLException
	{
		return false;
	}

	/**
	 * Retrieves whether it is possible to have multiple <code>ResultSet</code>
	 * objects returned from a <code>CallableStatement</code> object
	 * simultaneously.
	 * 
	 * @return <code>true</code> if a <code>CallableStatement</code> object
	 *         can return multiple <code>ResultSet</code> objects
	 *         simultaneously; <code>false</code> otherwise
	 * @exception SQLException if a datanase access error occurs
	 * @since 1.4
	 */
	public boolean supportsMultipleOpenResults() throws SQLException
	{
		return false;
	}

	/**
	 * Retrieves whether auto-generated keys can be retrieved after a statement
	 * has been executed.
	 * 
	 * @return <code>true</code> if auto-generated keys can be retrieved after
	 *         a statement has executed; <code>false</code> otherwise
	 * @exception SQLException if a database access error occurs
	 * @since 1.4
	 */
	public boolean supportsGetGeneratedKeys() throws SQLException
	{
		return false;
	}

	/**
	 * Retrieves a description of the user-defined type (UDT) hierarchies
	 * defined in a particular schema in this database. Only the immediate super
	 * type/ sub type relationship is modeled.
	 * <P>
	 * Only supertype information for UDTs matching the catalog, schema, and
	 * type name is returned. The type name parameter may be a fully-qualified
	 * name. When the UDT name supplied is a fully-qualified name, the catalog
	 * and schemaPattern parameters are ignored.
	 * <P>
	 * If a UDT does not have a direct super type, it is not listed here. A row
	 * of the <code>ResultSet</code> object returned by this method describes
	 * the designated UDT and a direct supertype. A row has the following
	 * columns:
	 * <OL>
	 * <LI><B>TYPE_CAT</B> String => the UDT's catalog (may be
	 * <code>null</code>)
	 * <LI><B>TYPE_SCHEM</B> String => UDT's schema (may be <code>null</code>)
	 * <LI><B>TYPE_NAME</B> String => type name of the UDT
	 * <LI><B>SUPERTYPE_CAT</B> String => the direct super type's catalog (may
	 * be <code>null</code>)
	 * <LI><B>SUPERTYPE_SCHEM</B> String => the direct super type's schema
	 * (may be <code>null</code>)
	 * <LI><B>SUPERTYPE_NAME</B> String => the direct super type's name
	 * </OL>
	 * <P>
	 * <B>Note:</B> If the driver does not support type hierarchies, an empty
	 * result set is returned.
	 * 
	 * @param catalog a catalog name; "" retrieves those without a catalog;
	 *            <code>null</code> means drop catalog name from the selection
	 *            criteria
	 * @param schemaPattern a schema name pattern; "" retrieves those without a
	 *            schema
	 * @param typeNamePattern a UDT name pattern; may be a fully-qualified name
	 * @return a <code>ResultSet</code> object in which a row gives
	 *         information about the designated UDT
	 * @throws SQLException if a database access error occurs
	 * @since 1.4
	 */
	public ResultSet getSuperTypes(String catalog, String schemaPattern,
		String typeNamePattern) throws SQLException
	{
		throw new UnsupportedFeatureException("DatabaseMetaData.getSuperTypes");
	}

	/**
	 * Retrieves a description of the table hierarchies defined in a particular
	 * schema in this database.
	 * <P>
	 * Only supertable information for tables matching the catalog, schema and
	 * table name are returned. The table name parameter may be a fully-
	 * qualified name, in which case, the catalog and schemaPattern parameters
	 * are ignored. If a table does not have a super table, it is not listed
	 * here. Supertables have to be defined in the same catalog and schema as
	 * the sub tables. Therefore, the type description does not need to include
	 * this information for the supertable.
	 * <P>
	 * Each type description has the following columns:
	 * <OL>
	 * <LI><B>TABLE_CAT</B> String => the type's catalog (may be
	 * <code>null</code>)
	 * <LI><B>TABLE_SCHEM</B> String => type's schema (may be
	 * <code>null</code>)
	 * <LI><B>TABLE_NAME</B> String => type name
	 * <LI><B>SUPERTABLE_NAME</B> String => the direct super type's name
	 * </OL>
	 * <P>
	 * <B>Note:</B> If the driver does not support type hierarchies, an empty
	 * result set is returned.
	 * 
	 * @param catalog a catalog name; "" retrieves those without a catalog;
	 *            <code>null</code> means drop catalog name from the selection
	 *            criteria
	 * @param schemaPattern a schema name pattern; "" retrieves those without a
	 *            schema
	 * @param tableNamePattern a table name pattern; may be a fully-qualified
	 *            name
	 * @return a <code>ResultSet</code> object in which each row is a type
	 *         description
	 * @throws SQLException if a database access error occurs
	 * @since 1.4
	 */
	public ResultSet getSuperTables(String catalog, String schemaPattern,
		String tableNamePattern) throws SQLException
	{
		throw new UnsupportedFeatureException("DatabaseMetaData.getSuperTables");
	}

	/**
	 * Retrieves a description of the given attribute of the given type for a
	 * user-defined type (UDT) that is available in the given schema and
	 * catalog.
	 * <P>
	 * Descriptions are returned only for attributes of UDTs matching the
	 * catalog, schema, type, and attribute name criteria. They are ordered by
	 * TYPE_SCHEM, TYPE_NAME and ORDINAL_POSITION. This description does not
	 * contain inherited attributes.
	 * <P>
	 * The <code>ResultSet</code> object that is returned has the following
	 * columns:
	 * <OL>
	 * <LI><B>TYPE_CAT</B> String => type catalog (may be <code>null</code>)
	 * <LI><B>TYPE_SCHEM</B> String => type schema (may be <code>null</code>)
	 * <LI><B>TYPE_NAME</B> String => type name
	 * <LI><B>ATTR_NAME</B> String => attribute name
	 * <LI><B>DATA_TYPE</B> short => attribute type SQL type from
	 * java.sql.Types
	 * <LI><B>ATTR_TYPE_NAME</B> String => Data source dependent type name.
	 * For a UDT, the type name is fully qualified. For a REF, the type name is
	 * fully qualified and represents the target type of the reference type.
	 * <LI><B>ATTR_SIZE</B> int => column size. For char or date types this is
	 * the maximum number of characters; for numeric or decimal types this is
	 * precision.
	 * <LI><B>DECIMAL_DIGITS</B> int => the number of fractional digits
	 * <LI><B>NUM_PREC_RADIX</B> int => Radix (typically either 10 or 2)
	 * <LI><B>NULLABLE</B> int => whether NULL is allowed
	 * <UL>
	 * <LI> attributeNoNulls - might not allow NULL values
	 * <LI> attributeNullable - definitely allows NULL values
	 * <LI> attributeNullableUnknown - nullability unknown
	 * </UL>
	 * <LI><B>REMARKS</B> String => comment describing column (may be
	 * <code>null</code>)
	 * <LI><B>ATTR_DEF</B> String => default value (may be <code>null</code>)
	 * <LI><B>SQL_DATA_TYPE</B> int => unused
	 * <LI><B>SQL_DATETIME_SUB</B> int => unused
	 * <LI><B>CHAR_OCTET_LENGTH</B> int => for char types the maximum number
	 * of bytes in the column
	 * <LI><B>ORDINAL_POSITION</B> int => index of column in table (starting
	 * at 1)
	 * <LI><B>IS_NULLABLE</B> String => "NO" means column definitely does not
	 * allow NULL values; "YES" means the column might allow NULL values. An
	 * empty string means unknown.
	 * <LI><B>SCOPE_CATALOG</B> String => catalog of table that is the scope
	 * of a reference attribute (<code>null</code> if DATA_TYPE isn't REF)
	 * <LI><B>SCOPE_SCHEMA</B> String => schema of table that is the scope of
	 * a reference attribute (<code>null</code> if DATA_TYPE isn't REF)
	 * <LI><B>SCOPE_TABLE</B> String => table name that is the scope of a
	 * reference attribute (<code>null</code> if the DATA_TYPE isn't REF)
	 * <LI><B>SOURCE_DATA_TYPE</B> short => source type of a distinct type or
	 * user-generated Ref type,SQL type from java.sql.Types (<code>null</code>
	 * if DATA_TYPE isn't DISTINCT or user-generated REF)
	 * </OL>
	 * 
	 * @param catalog a catalog name; must match the catalog name as it is
	 *            stored in the database; "" retrieves those without a catalog;
	 *            <code>null</code> means that the catalog name should not be
	 *            used to narrow the search
	 * @param schemaPattern a schema name pattern; must match the schema name as
	 *            it is stored in the database; "" retrieves those without a
	 *            schema; <code>null</code> means that the schema name should
	 *            not be used to narrow the search
	 * @param typeNamePattern a type name pattern; must match the type name as
	 *            it is stored in the database
	 * @param attributeNamePattern an attribute name pattern; must match the
	 *            attribute name as it is declared in the database
	 * @return a <code>ResultSet</code> object in which each row is an
	 *         attribute description
	 * @exception SQLException if a database access error occurs
	 * @since 1.4
	 */
	public ResultSet getAttributes(String catalog, String schemaPattern,
		String typeNamePattern, String attributeNamePattern)
	throws SQLException
	{
		throw new UnsupportedFeatureException("getAttributes");
	}

	/**
	 * Retrieves whether this database supports the given result set
	 * holdability.
	 * 
	 * @param holdability one of the following constants:
	 *            <code>ResultSet.HOLD_CURSORS_OVER_COMMIT</code> or
	 *            <code>ResultSet.CLOSE_CURSORS_AT_COMMIT<code>
	 * @return <code>true</code> if so; <code>false</code> otherwise
	 * @exception SQLException if a database access error occurs
	 * @see Connection
	 * @since 1.4
	 */
	public boolean supportsResultSetHoldability(int holdability)
	throws SQLException
	{
		return true;
	}

	/**
	 * Retrieves the default holdability of this <code>ResultSet</code>
	 * object.
	 * 
	 * @return the default holdability; either
	 *         <code>ResultSet.HOLD_CURSORS_OVER_COMMIT</code> or
	 *         <code>ResultSet.CLOSE_CURSORS_AT_COMMIT</code>
	 * @exception SQLException if a database access error occurs
	 * @since 1.4
	 */
	public int getResultSetHoldability() throws SQLException
	{
		return ResultSet.HOLD_CURSORS_OVER_COMMIT;
	}

	/**
	 * Retrieves the major version number of the underlying database.
	 * 
	 * @return the underlying database's major version
	 * @exception SQLException if a database access error occurs
	 * @since 1.4
	 */
	public int getDatabaseMajorVersion() throws SQLException
	{
		return m_connection.getVersionNumber()[0];
	}

	/**
	 * Retrieves the minor version number of the underlying database.
	 * 
	 * @return underlying database's minor version
	 * @exception SQLException if a database access error occurs
	 * @since 1.4
	 */
	public int getDatabaseMinorVersion() throws SQLException
	{
		return m_connection.getVersionNumber()[1];
	}

	/**
	 * Retrieves the major JDBC version number for this driver.
	 * 
	 * @return JDBC version major number
	 * @exception SQLException if a database access error occurs
	 * @since 1.4
	 */
	public int getJDBCMajorVersion() throws SQLException
	{
		return 3; // This class implements JDBC 3.0
	}

	/**
	 * Retrieves the minor JDBC version number for this driver.
	 * 
	 * @return JDBC version minor number
	 * @exception SQLException if a database access error occurs
	 * @since 1.4
	 */
	public int getJDBCMinorVersion() throws SQLException
	{
		return 0; // This class implements JDBC 3.0
	}

	/**
	 * Indicates whether the SQLSTATEs returned by
	 * <code>SQLException.getSQLState</code> is X/Open (now known as Open
	 * Group) SQL CLI or SQL99.
	 * 
	 * @return the type of SQLSTATEs, one of: sqlStateXOpen or sqlStateSQL99
	 * @throws SQLException if a database access error occurs
	 * @since 1.4
	 */
	public int getSQLStateType() throws SQLException
	{
		return DatabaseMetaData.sqlStateSQL99;
	}

	/**
	 * Indicates whether updates made to a LOB are made on a copy or directly to
	 * the LOB.
	 * 
	 * @return <code>true</code> if updates are made to a copy of the LOB;
	 *         <code>false</code> if updates are made directly to the LOB
	 * @throws SQLException if a database access error occurs
	 * @since 1.4
	 */
	public boolean locatorsUpdateCopy() throws SQLException
	{
		/*
		 * Currently LOB's aren't updateable at all, so it doesn't matter what
		 * we return. We don't throw the notImplemented Exception because the
		 * 1.5 JDK's CachedRowSet calls this method regardless of wether large
		 * objects are used.
		 */
		return true;
	}

	/**
	 * Retrieves weather this database supports statement pooling.
	 * 
	 * @return <code>true</code> is so; <code>false</code> otherwise
	 * @throws SQLExcpetion if a database access error occurs
	 * @since 1.4
	 */
	public boolean supportsStatementPooling() throws SQLException
	{
		return false;
	}

	/**
	 * Indicates whether or not this data source supports the SQL ROWID type,
	 * and if so the lifetime for which a RowId object remains valid.
	 *
	 * The returned int values have the following relationship:
	 *
	 *    ROWID_UNSUPPORTED < ROWID_VALID_OTHER < ROWID_VALID_TRANSACTION
	 *        < ROWID_VALID_SESSION < ROWID_VALID_FOREVER
	 *
	 * so conditional logic such as:
	 *
     *   if (metadata.getRowIdLifetime() > DatabaseMetaData.ROWID_VALID_TRANSACTION)
     *
	 * can be used. Valid Forever means valid across all Sessions, and valid for
	 * a Session means valid across all its contained Transactions.
	 *
	 * @return the status indicating the lifetime of a RowId 
	 * @throws SQLException - if a database access error occurs
	 * @since 1.6
	 */
	public RowIdLifetime getRowIdLifetime()
	throws SQLException
	{
		return RowIdLifetime.ROWID_UNSUPPORTED;
	}

	/**
	 * Retrieves whether this database supports invoking user-defined or vendor
	 * functions using the stored procedure escape syntax.
	 *
	 * @return true is so; false otherwise
	 * @throw SQLException - if database access error occurs
	 * @since 1.6
	 */
	public boolean supportsStoredFunctionsUsingCallSyntax()
	throws SQLException
	{
		return false;
	}

	/**
	 * Retrieves whether a SQLException while autoCommit is true inidcates that
	 * all open ResultSets are closed, even ones that are holdable. When a
	 * SQLException occurs while autocommit is true, it is vendor specific
	 * whether the JDBC driver responds with a commit operation, a rollback
	 * operation, or by doing neither a commit nor a rollback. A potential
	 * result of this difference is in whether or not holdable ResultSets are
	 * closed.
	 *
	 * @return true if so; false otherwise 
	 * @throw SQLException - if a database access error occurs
	 * @since 1.6
	*/
	public boolean autoCommitFailureClosesAllResultSets()
    throws SQLException
	{
		return false;
	}


	/**
	 * Retrieves a list of the client info properties that the driver
	 * supports. The result set contains the following columns
	 *
     *   1. NAME          String => The name of the client info property
     *   2. MAX_LEN          int => The maximum length of the value
     *   3. DEFAULT_VALUE String => The default value of the property
     *   4. DESCRIPTION   String => A description of the property
	 *      This will typically contain information as to where this property 
	 *      is stored in the database. 
	 *
	 * The ResultSet is sorted by the NAME column
	 *
	 * @return A ResultSet object; each row is a supported client info property
	 * @throw SQLException - if a database access error occurs
	 * @since 1.6
	 */
	public ResultSet getClientInfoProperties()
	throws SQLException
	{
		throw new UnsupportedFeatureException(
              "DatabaseMetaData.getClientInfoProperties");
	}

	/**
	 * Retrieves a description of the system and user functions available in the
	 * given catalog.
	 *
	 * Only system and user function descriptions matching the schema and
	 * function name criteria are returned. They are ordered by FUNCTION_CAT,
	 * FUNCTION_SCHEM, FUNCTION_NAME and SPECIFIC_ NAME.
	 *
	 * Each function description has the the following columns:
	 *
	 *   1. FUNCTION_CAT   String => function catalog (may be null)
     *   2. FUNCTION_SCHEM String => function schema (may be null)
     *   3. FUNCTION_NAME  String => function name.
     *   4. REMARKS        String => explanatory comment on the function
     *   5. FUNCTION_TYPE   short => kind of function:
	 *        - functionResultUnknown : Return type unknown
     *        - functionNoTable       : Does not return a table
     *        - functionReturnsTable  : Returns a table
	 *   6. SPECIFIC_NAME  String => the name which uniquely identifies this
	 *      function within its schema. This is a user specified, or DBMS
	 *      generated, name that may be different from the FUNCTION_NAME for
	 *      example with overload functions.
	 *
	 * A user may not have permission to execute any of the functions that are
	 * returned by getFunctions
	 *
	 * @return ResultSet    - each row is a function description 
	 * @throw  SQLException - if a database access error occurs
	 * @since  1.6
	 */
	public ResultSet getFunctions(String catalog, 
								  String schemaPattern, 
								  String functionNamePattern)
	throws SQLException
	{
		throw new UnsupportedFeatureException("DatabaseMetaData.getFunctions");
	}

	/**
	 * Retrieves a description of the given catalog's system or user function
	 * parameters and return type.
	 *
	 * Only descriptions matching the schema, function and parameter name
	 * criteria are returned. They are ordered by FUNCTION_CAT, FUNCTION_SCHEM,
	 * FUNCTION_NAME and SPECIFIC_ NAME. Within this, the return value, if any,
	 * is first. Next are the parameter descriptions in call order. The column
	 * descriptions follow in column number order.
	 *
	 * Each row in the ResultSet is a parameter description, column description
	 * or return type description with the following fields:
	 *
     *   1. FUNCTION_CAT   String => function catalog (may be null)
     *   2. FUNCTION_SCHEM String => function schema (may be null)
     *   3. FUNCTION_NAME  String => function name used to invoke the function
     *   4. COLUMN_NAME    String => column/parameter name
     *   5. COLUMN_TYPE     Short => kind of column/parameter:
     *        - functionColumnUnknown : nobody knows
     *        - functionColumnIn      : IN parameter
     *        - functionColumnInOut   : INOUT parameter
     *        - functionColumnOut     : OUT parameter
     *        - functionColumnReturn  : function return value
     *        - functionColumnResult  : Indicates that the parameter or column 
	 *                                  is a column in the ResultSet 
	 *   6. DATA_TYPE    int => SQL type from java.sql.Types
	 *   7. TYPE_NAME String => SQL type name (UDT fully qualified)
	 *   8. PRECISION    int => precision
     *   9. LENGTH       int => length in bytes of data 
     *  10. SCALE      short => scale (NULL when NA)
	 *  11. RADIX      short => radix
     *  12. NULLABLE short => can it contain NULL.
     *        - functionNoNulls - does not allow NULL values
     *        - functionNullable - allows NULL values
     *        - functionNullableUnknown - nullability unknown 
     *  13. REMARKS String => comment describing column/parameter
     *  14. CHAR_OCTET_LENGTH int => the maximum length of binary and character
	 *      based parameters or columns. For any other datatype the returned 
	 *      value is a NULL
     *  15. ORDINAL_POSITION int => the ordinal position, starting from 1, for
     *      the input and output parameters. A value of 0 is returned if this
     *      row describes the function's return value. For result set columns,
     *      it is the ordinal position of the column in the result set starting
     *      from 1.
     *  16. IS_NULLABLE String => ISO rules are used to determine the 
	 *      nullability for a parameter or column.
     *        - YES  : if the parameter or column can include NULLs
     *        - NO   : if the parameter or column cannot include NULLs
     *        - ""   : if the nullability for the parameter or column is unknown
     *  17. SPECIFIC_NAME String => the name which uniquely identifies this 
	 *      function within its schema. This is a user specified, or DBMS 
	 *      generated, name that may be different then the FUNCTION_NAME for 
	 *      example with overload functions.
	 *
	 * The PRECISION column represents the specified column size for the given
	 * parameter or column. For numeric data, this is the maximum precision. For
	 * character data, this is the length in characters. For datetime datatypes,
	 * this is the length in characters of the String representation (assuming
	 * the maximum allowed precision of the fractional seconds component). For
	 * binary data, this is the length in bytes. For the ROWID datatype, this is
	 * the length in bytes. Null is returned for data types where the column
	 * size is not applicable.
	 *
	 * @param catalog - a catalog name; must match the catalog name as it is
	 *         stored in the database; "" retrieves those without a catalog;
	 *         null means that the catalog name should not be used to narrow the
	 *         search
	 * @param schemaPattern - a schema name pattern; must match the schema name
	 *         as it is stored in the database; "" retrieves those without a
	 *         schema; null means that the schema name should not be used to
	 *         narrow the search
	 * @param functionNamePattern - a procedure name pattern; must match the
	 *         function name as it is stored in the database
     * @param columnNamePattern - a parameter name pattern; must match the
     *         parameter or column name as it is stored in the database
	 * @return ResultSet - each row describes a user function parameter, column
	 *         or return type
	 * @throw  SQLException - if a database access error occurs
	 * @since  1.6
	*/
	public ResultSet getFunctionColumns(String catalog,
										String schemaPattern,
										String functionNamePattern,
										String columnNamePattern)
	throws SQLException
	{
		throw new UnsupportedFeatureException("DatabaseMetaData.getFunctionColumns");
	}

	/**
	 * This method creates a ResultSet which is not associated with any
	 * statement.
	 */
	private ResultSet createSyntheticResultSet(ResultSetField[] f, ArrayList tuples)
	throws SQLException
	{
		return new SyntheticResultSet(f, tuples);
	}


	/**
	 * Returns true if this either implements the interface argument or is
	 * directly or indirectly a wrapper for an object that does. Returns false
	 * otherwise. If this implements the interface then return true, else if
	 * this is a wrapper then return the result of recursively calling
	 * isWrapperFor on the wrapped object. If this does not implement the
	 * interface and is not a wrapper, return false. This method should be
	 * implemented as a low-cost operation compared to unwrap so that callers
	 * can use this method to avoid expensive unwrap calls that may fail. If
	 * this method returns true then calling unwrap with the same argument
	 * should succeed.
	 *
	 * @since 1.6
	 */
	public boolean isWrapperFor(Class iface)
	throws SQLException
	{
		throw new UnsupportedFeatureException("isWrapperFor");
	}

	/**
	 * Returns an object that implements the given interface to allow access to
	 * non-standard methods, or standard methods not exposed by the proxy. If
	 * the receiver implements the interface then the result is the receiver or
	 * a proxy for the receiver. If the receiver is a wrapper and the wrapped
	 * object implements the interface then the result is the wrapped object or
	 * a proxy for the wrapped object. Otherwise return the the result of
	 * calling unwrap recursively on the wrapped object or a proxy for that
	 * result. If the receiver is not a wrapper and does not implement the
	 * interface, then an SQLException is thrown.
	 *
	 * @since 1.6
	 */
	public Object unwrap(Class iface)
	throws SQLException
	{
		throw new UnsupportedFeatureException("unwrap");
	}
}
