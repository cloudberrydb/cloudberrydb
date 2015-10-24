/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.jdbc;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.BitSet;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.postgresql.pljava.jdbc.BlobValue;
import org.postgresql.pljava.jdbc.ClobValue;
import org.postgresql.pljava.internal.Oid;
import org.postgresql.pljava.internal.PgSavepoint;

/**
 * @author Thomas Hallgren
 */
public class SPIConnection implements Connection
{
	private static final HashMap s_sqlType2Class = new HashMap(30);
    private int[] VERSION_NUMBER = null; //version number

	static
	{
		addType(String.class, Types.VARCHAR);
		addType(Byte.class, Types.TINYINT);
		addType(Short.class, Types.SMALLINT);
		addType(Integer.class, Types.INTEGER);
		addType(Long.class, Types.BIGINT);
		addType(Float.class, Types.FLOAT);
		addType(Double.class, Types.DOUBLE);
		addType(BigDecimal.class, Types.DECIMAL);
		addType(BigInteger.class, Types.NUMERIC);
		addType(Boolean.class, Types.BOOLEAN);
		addType(Blob.class, Types.BLOB);
		addType(Clob.class, Types.CLOB);
		addType(Date.class, Types.DATE);
		addType(Time.class, Types.TIME);
		addType(Timestamp.class, Types.TIMESTAMP);
		addType(java.util.Date.class, Types.TIMESTAMP);
		addType(byte[].class, Types.VARBINARY);
		addType(BitSet.class, Types.BIT);
		addType(URL.class, Types.DATALINK);
	}

	private static final void addType(Class clazz, int sqlType)
	{
		s_sqlType2Class.put(clazz, new Integer(sqlType));
	}

	/**
	 * Returns a default connection instance. It is the callers responsability
	 * to close this instance.
	 */
	public static Connection getDefault()
	throws SQLException
	{
		return new SPIConnection();
	}

	/**
	 * Returns {@link ResultSet#CLOSE_CURSORS_AT_COMMIT}. Cursors are actually
	 * closed when a function returns to SQL.
	 */
	public int getHoldability() throws SQLException
	{
		return ResultSet.CLOSE_CURSORS_AT_COMMIT;
	}

	/**
	 * Returns {@link Connection#TRANSACTION_READ_COMMITTED}.
	 */
	public int getTransactionIsolation() throws SQLException
	{
		return TRANSACTION_READ_COMMITTED;
	}

	/**
	 * Warnings are not yet supported.
	 * @throws SQLException indicating that this feature is not supported.
	 */
	public void clearWarnings()
	throws SQLException
	{
		throw new UnsupportedFeatureException("Connection.clearWarnings");
	}

	/**
	 * This is a no-op. The default connection never closes.
	 */
	public void close()
	throws SQLException
	{
	}

	/**
	 * It's not legal to do a commit within a call from SQL.
	 * @throws SQLException indicating that this feature is not supported.
	 */
	public void commit()
	throws SQLException
	{
		throw new UnsupportedFeatureException("Connection.commit");
	}

	/**
	 * It's not legal to do a rollback within a call from SQL.
	 * @throws SQLException indicating that this feature is not supported.
	 */
	public void rollback()
	throws SQLException
	{
		throw new UnsupportedFeatureException("Connection.rollback");
	}

	/**
	 * It is assumed that an SPI call is under transaction control. This method
	 * will always return <code>false</code>.
	 */
	public boolean getAutoCommit()
	throws SQLException
	{
		return false;
	}

	/**
	 * Will always return false.
	 */
	public boolean isClosed()
	throws SQLException
	{
		return false;
	}

	/**
	 * Will always return true
	 */
	public boolean isValid(int timeout)
    throws SQLException
	{
		return true;
	}

	/**
	 * Returns <code>false</code>. The SPIConnection is not real-only.
	 */
	public boolean isReadOnly()
	throws SQLException
	{
		return false;
	}

	/**
	 * Change of holdability is not supported.
	 * @throws SQLException indicating that this feature is not supported.
	 */
	public void setHoldability(int holdability)
	throws SQLException
	{
		throw new UnsupportedFeatureException("Connection.setHoldability");
	}

	/**
	 * Change of transaction isolation level is not supported.
	 * @throws SQLException indicating that this feature is not supported.
	 */
	public void setTransactionIsolation(int level)
	throws SQLException
	{
		throw new UnsupportedFeatureException("Connection.setTransactionIsolation");
	}

	/**
	 * It is assumed that an SPI call is under transaction control. Changing
	 * that is not supported.
	 * @throws SQLException indicating that this feature is not supported.
	 */
	public void setAutoCommit(boolean autoCommit)
	throws SQLException
	{
		throw new UnsupportedFeatureException("Connection.setAutoCommit");
	}

	/**
	 * It is assumed that an inserts and updates can be performed using and
	 * SPIConnection. Changing that is not supported. 
	 * @throws SQLException indicating that this feature is not supported.
	 */
	public void setReadOnly(boolean readOnly)
	throws SQLException
	{
		throw new UnsupportedFeatureException("Connection.setReadOnly");
	}

	/**
	 * Returns <code>null</code>.
	 */
	public String getCatalog()
	throws SQLException
	{
		return null;
	}

	/**
	 * The catalog name cannot be set.
	 * @throws SQLException indicating that this feature is not supported.
	 */
	public void setCatalog(String catalog)
	throws SQLException
	{
		throw new UnsupportedFeatureException("Connection.setCatalog");
	}

	public String getClientInfo(String name)
    throws SQLException
	{
		throw new UnsupportedFeatureException("ClientInfo is not yet supported");
	}

	public Properties getClientInfo()
    throws SQLException
	{
		throw new UnsupportedFeatureException("ClientInfo is not yet supported");
	}

	public void setClientInfo(Properties properties)
	throws SQLClientInfoException
	{
		throw new SQLClientInfoException();
	}

	public void setClientInfo(String name, String value)
	throws SQLClientInfoException
	{
		throw new SQLClientInfoException();
	}


	/**
	 * DatabaseMetaData is not yet supported.
	 * @throws SQLException indicating that this feature is not supported.
	 */
	public DatabaseMetaData getMetaData()
	throws SQLException
	{
		return new SPIDatabaseMetaData(this);
	}

	/**
	 * Warnings are not yet supported.
	 * @throws SQLException indicating that this feature is not supported.
	 */
	public SQLWarning getWarnings()
	throws SQLException
	{
		throw new UnsupportedFeatureException("Connection.getWarnings");
	}

	public void releaseSavepoint(Savepoint savepoint) throws SQLException
	{
		if(!(savepoint instanceof PgSavepoint))
			throw new IllegalArgumentException("Not a PL/Java Savepoint");

		PgSavepoint sp = (PgSavepoint)savepoint;
		sp.release();
		forgetSavepoint(sp);
	}

	public void rollback(Savepoint savepoint) throws SQLException
	{
		if(!(savepoint instanceof PgSavepoint))
			throw new IllegalArgumentException("Not a PL/Java Savepoint");

		PgSavepoint sp = (PgSavepoint)savepoint;
		Invocation.clearErrorCondition();
		sp.rollback();
		forgetSavepoint(sp);
	}

	/**
	 * Creates a new instance of <code>SPIStatement</code>.
	 */
	public Statement createStatement()
	throws SQLException
	{
		if(this.isClosed())
			throw new SQLException("Connection is closed");
		return new SPIStatement(this);
	}

	/**
	 * Creates a new instance of <code>SPIStatement</code>.
	 * 
	 * @throws SQLException
	 *             if the <code>resultSetType</code> differs from {@link
	 *             ResultSet#TYPE_FORWARD_ONLY} or if the <code>resultSetConcurrencty</code>
	 *             differs from {@link ResultSet#CONCUR_READ_ONLY}.
	 */
	public Statement createStatement(
		int resultSetType,
		int resultSetConcurrency)
		throws SQLException
	{
		if (resultSetType != ResultSet.TYPE_FORWARD_ONLY)
			throw new UnsupportedOperationException("TYPE_FORWARD_ONLY supported ResultSet type");

		if(resultSetConcurrency != ResultSet.CONCUR_READ_ONLY)
			throw new UnsupportedOperationException("CONCUR_READ_ONLY is the supported ResultSet concurrency");
		return this.createStatement();
	}

	/**
	 * Creates a new instance of <code>SPIStatement</code>.
	 * 
	 * @throws SQLException
	 *             if the <code>resultSetType</code> differs from {@link
	 *             ResultSet#TYPE_FORWARD_ONLY}, if the <code>resultSetConcurrencty</code>
	 *             differs from {@link ResultSet#CONCUR_READ_ONLY}, or if the
	 *             resultSetHoldability differs from {@link ResultSet#CLOSE_CURSORS_AT_COMMIT}.
	 */
	public Statement createStatement(
		int resultSetType,
		int resultSetConcurrency,
		int resultSetHoldability)
		throws SQLException
	{
		if(resultSetHoldability != ResultSet.CLOSE_CURSORS_AT_COMMIT)
			throw new UnsupportedOperationException(
					"CLOSE_CURSORS_AT_COMMIT is the only supported ResultSet holdability");
		return this.createStatement(resultSetType, resultSetConcurrency);
	}

	/**
	 * Factory method for creating Array objects.
	 *
	 * @throws SQLException indicating that this feature is not supported.
	 */
	public Array createArrayOf(String typeName, Object[] elements)
	throws SQLException
	{
		throw new UnsupportedFeatureException("Array objects not yet implemented");
	}

	/**
	 * Constructs an object that implements the Blob interface. 
	 *
	 * The object returned initially contains no data. The setAsciiStream,
	 * setCharacterStream and setString methods of the Blob interface may be
	 * used to add data to the Blob.
	 */
	public Blob createBlob()
	throws SQLException
	{
		return new BlobValue(new byte[0]);
	}

	/**
	 * Constructs an object that implements the Clob interface. 
	 *
	 * The object returned initially contains no data. The setAsciiStream,
	 * setCharacterStream and setString methods of the Clob interface may be
	 * used to add data to the Clob.
	 */
	public Clob createClob()
	throws SQLException
	{
		return new ClobValue("");
	}

	/**
	 * Constructs an object that implements the NClob interface. 
	 *
	 * The object returned initially contains no data. The setAsciiStream,
	 * setCharacterStream and setString methods of the NClob interface may be
	 * used to add data to the NClob.
	 */
	public NClob createNClob()
	throws SQLException
	{
		throw new UnsupportedFeatureException("NClob objects not yet implemented");
	}

	/**
	 * Constructs an object that implemnts the SQLXML interface.  
	 *
	 * The object returned initially contains no data. The createXmlStreamWriter
	 * object and setString method of the SQLXML interface may be used to add
	 * data to the SQLXML object.
	 */
	public SQLXML createSQLXML()
	throws SQLException
	{
		throw new UnsupportedFeatureException("SQLXML objects not yet implemented");
	}

	/**
	 * Factory method for creating Struct objects.
	 *
	 * @throws SQLException indicating that this feature is not supported.
	 */
	public Struct createStruct(String typeName, Object[] attributes)
	throws SQLException
	{
		throw new UnsupportedFeatureException("Struct objects not yet implemented");
	}	

	/**
	 * Returns <code>null</code>. Type map is not yet imlemented.
	 */
	public Map getTypeMap()
	throws SQLException
	{
		return null;
	}

	/**
	 * Type map is not yet implemented.
	 * @throws SQLException indicating that this feature is not supported.
	 */
	public void setTypeMap(Map map)
	throws SQLException
	{
		throw new UnsupportedOperationException("Type map is not yet implemented");
	}

	/**
	 * Parse the JDBC SQL into PostgreSQL.
	 */
	public String nativeSQL(String sql)
	throws SQLException
	{
		return this.nativeSQL(sql, null);
	}
	
	public String nativeSQL(String sql, int[] paramCountRet)
	{
		StringBuffer buf = new StringBuffer();
		int len = sql.length();
		char inQuote = 0;
		int paramIndex = 1;
		for(int idx = 0; idx < len; ++idx)
		{
			char c = sql.charAt(idx);
			switch(c)
			{
			case '\\':
				// Next character is escaped. Keep both
				// escape and the character.
				//
				buf.append(c);
				if(++idx == len)
					break;
				c = sql.charAt(idx);
				break;

			case '\'':
			case '"':
				// Strings within quotes should not be subject
				// to '?' -> '$n' substitution.
				//
				if(inQuote == c)
					inQuote = 0;
				else
					inQuote = c;
				break;
			
			case '?':
				if(inQuote == 0)
				{
					buf.append('$');
					buf.append(paramIndex++);
					continue;
				}
				break;
			
			default:
				if(inQuote == 0 && Character.isWhitespace(c))
				{
					// Strip of multiple whitespace outside of
					// strings.
					//
					++idx;
					while(idx < len && Character.isWhitespace(sql.charAt(idx)))
						++idx;
					--idx;
					c = ' ';
				}
			}
			buf.append(c);
		}
		if(paramCountRet != null)
			paramCountRet[0] = paramIndex - 1;
		return buf.toString();
	}

	/**
	 * Procedure calls are not yet implemented.
	 * @throws SQLException indicating that this feature is not supported.
	 */
	public CallableStatement prepareCall(String sql) throws SQLException
	{
		throw new UnsupportedOperationException("Procedure calls are not yet implemented");
	}

	/**
	 * Procedure calls are not yet implemented.
	 * @throws SQLException indicating that this feature is not supported.
	 */
	public CallableStatement prepareCall(
		String sql,
		int resultSetType,
		int resultSetConcurrency)
		throws SQLException
	{
		throw new UnsupportedOperationException("Procedure calls are not yet implemented");
	}

	/**
	 * Procedure calls are not yet implemented.
	 * @throws SQLException indicating that this feature is not supported.
	 */
	public CallableStatement prepareCall(
		String sql,
		int resultSetType,
		int resultSetConcurrency,
		int resultSetHoldability)
		throws SQLException
	{
		throw new UnsupportedOperationException("Procedure calls are not yet implemented");
	}

	/**
	 * Creates a new instance of <code>SPIPreparedStatement</code>.
	 */
	public PreparedStatement prepareStatement(String sql)
	throws SQLException
	{
		if(this.isClosed())
			throw new SQLException("Connection is closed");

		int[] pcount = new int[] { 0 };
		sql = this.nativeSQL(sql, pcount);
		PreparedStatement stmt = new SPIPreparedStatement(this, sql, pcount[0]);
		Invocation.current().manageStatement(stmt);
		return stmt;
	}

	/**
	 * Return of auto generated keys is not yet supported.
	 * @throws SQLException indicating that this feature is not supported.
	 */
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
	throws SQLException
	{
		throw new UnsupportedFeatureException("Auto generated key support not yet implemented");
	}

	/**
	 * Creates a new instance of <code>SPIPreparedStatement</code>.
	 * 
	 * @throws SQLException
	 *             if the <code>resultSetType</code> differs from {@link
	 *             ResultSet#TYPE_FORWARD_ONLY} or if the <code>resultSetConcurrencty</code>
	 *             differs from {@link ResultSet#CONCUR_READ_ONLY}.
	 */
	public PreparedStatement prepareStatement(
		String sql,
		int resultSetType,
		int resultSetConcurrency)
		throws SQLException
	{
		if(resultSetType != ResultSet.TYPE_FORWARD_ONLY)
			throw new UnsupportedOperationException("TYPE_FORWARD_ONLY supported ResultSet type");

		if(resultSetConcurrency != ResultSet.CONCUR_READ_ONLY)
			throw new UnsupportedOperationException("CONCUR_READ_ONLY is the supported ResultSet concurrency");
		return prepareStatement(sql);
	}

	/**
	 * Creates a new instance of <code>SPIPreparedStatement</code>.
	 * 
	 * @throws SQLException
	 *             if the <code>resultSetType</code> differs from {@link
	 *             ResultSet#TYPE_FORWARD_ONLY}, if the <code>resultSetConcurrencty</code>
	 *             differs from {@link ResultSet#CONCUR_READ_ONLY}, or if the
	 *             resultSetHoldability differs from {@link ResultSet#CLOSE_CURSORS_AT_COMMIT}.
	 */
	public PreparedStatement prepareStatement(
		String sql,
		int resultSetType,
		int resultSetConcurrency,
		int resultSetHoldability)
		throws SQLException
	{
		if(resultSetHoldability != ResultSet.CLOSE_CURSORS_AT_COMMIT)
			throw new UnsupportedOperationException(
			"CLOSE_CURSORS_AT_COMMIT is the only supported ResultSet holdability");
		return this.prepareStatement(sql, resultSetType, resultSetConcurrency);
	}

	/**
	 * Return of auto generated keys is not yet supported.
	 * @throws SQLException indicating that this feature is not supported.
	 */
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
	throws SQLException
	{
		throw new UnsupportedFeatureException("Auto generated key support not yet implemented");
	}

	/**
	 * Return of auto generated keys is not yet supported.
	 * @throws SQLException indicating that this feature is not supported.
	 */
	public PreparedStatement prepareStatement(String sql, String[] columnNames)
	throws SQLException
	{
		throw new UnsupportedFeatureException("Auto generated key support not yet implemented");
	}

	public Savepoint setSavepoint()
	throws SQLException
	{
		return this.rememberSavepoint(PgSavepoint.set("anonymous_savepoint"));
	}

	public Savepoint setSavepoint(String name)
	throws SQLException
	{
		return this.rememberSavepoint(PgSavepoint.set(name));
	}

	static int getTypeForClass(Class c)
	{
		if(c.isArray() && !c.equals(byte[].class))
			return Types.ARRAY;

		Integer sqt = (Integer)s_sqlType2Class.get(c);
		if(sqt != null)
			return sqt.intValue();

		/*
		 * This is not a well known JDBC type.
		 */
		return Types.OTHER;
	}

	private Savepoint rememberSavepoint(PgSavepoint sp)
	throws SQLException
	{
		// Remember the first savepoint for each call-level so
		// that it can be released when the function call ends. Releasing
		// the first savepoint will release all subsequent savepoints.
		//
		Invocation invocation = Invocation.current();
		Savepoint old = invocation.getSavepoint();
		if(old == null)
			invocation.setSavepoint(sp);
		return sp;
	}

	private static void forgetSavepoint(PgSavepoint sp)
	throws SQLException
	{
		Invocation invocation = Invocation.current();
		if(invocation.getSavepoint() == sp)
			invocation.setSavepoint(null);
	}

    public int[] getVersionNumber() throws SQLException
    {
        if (VERSION_NUMBER != null)
        	return VERSION_NUMBER;

        ResultSet rs = createStatement().executeQuery(
            "SELECT version()");

        try
        {
            if (!rs.next())
                throw new SQLException(
                "Cannot retrieve product version number");

            String ver = rs.getString(1);
            Pattern p = Pattern.compile(
                "^PostgreSQL\\s+(\\d+)\\.(\\d+)(.\\d+)?.*");
            Matcher m = p.matcher(ver);
            if(m.matches())
            {
            	VERSION_NUMBER = new int[3];
            	VERSION_NUMBER[0] = Integer.parseInt(m.group(1));
            	VERSION_NUMBER[1] = Integer.parseInt(m.group(2));
            	String bugfix = m.group(3);
            	if(bugfix != null && bugfix.length() > 1)
            		VERSION_NUMBER[2] = Integer.parseInt(bugfix.substring(1));
                return VERSION_NUMBER;
            }
            throw new SQLException(
                "Unexpected product version string format: " +
                ver);
        }
        catch (PatternSyntaxException e)
        {
            throw new SQLException(
                "Error in product version string parsing: " +
                e.getMessage());
        }
        finally
        {
            rs.close();
        }
    }

    /*
     * This implemetation uses the jdbc3Types array to support the jdbc3
     * datatypes.  Basically jdbc2 and jdbc3 are the same, except that
     * jdbc3 adds some
     */
    public int getSQLType(String pgTypeName)
    {
        if (pgTypeName == null)
            return Types.OTHER;

        for (int i = 0;i < JDBC3_TYPE_NAMES.length;i++)
            if (pgTypeName.equals(JDBC3_TYPE_NAMES[i]))
                return JDBC_TYPE_NUMBERS[i];

        return Types.OTHER;
    }

    /*
     * This returns the java.sql.Types type for a PG type oid
     *
     * @param oid PostgreSQL type oid
     * @return the java.sql.Types type
     * @exception SQLException if a database access error occurs
     */
    public int getSQLType(Oid oid) throws SQLException
    {
        return getSQLType(getPGType(oid));
    }
 
    public String getPGType(Oid oid) throws SQLException
    {
        String typeName = null;
        PreparedStatement query = null;
        ResultSet rs = null;

        try
        {
            query = prepareStatement("SELECT typname FROM pg_catalog.pg_type WHERE oid=?");
            query.setObject(1, oid);
            rs = query.executeQuery();

            if (rs.next())
            {
                typeName = rs.getString(1);
            }
            else
            {
                throw new SQLException("Cannot find PG type with oid=" + oid);
            }
        }
        finally
        {
            if (query != null)
            {
                query.close();
            }
        }

        return typeName;
    }

	static Object basicCoersion(Class cls, Object value)
	throws SQLException
	{
		if(value == null || cls.isInstance(value))
			return value;

		if(cls == String.class)
		{
			if(value instanceof Number
			|| value instanceof Boolean
			|| value instanceof Timestamp
			|| value instanceof Date
			|| value instanceof Time)
				return value.toString();
		}
		else if(cls == URL.class && value instanceof String)
		{
			try
			{
				return new URL((String)value);
			}
			catch(MalformedURLException e)
			{
				throw new SQLException(e.toString());
			}
		}
		throw new SQLException("Cannot derive a value of class " +
				cls.getName() + " from an object of class " + value.getClass().getName());
	}

	static Number basicNumericCoersion(Class cls, Object value)
	throws SQLException
	{
		if(value == null || value instanceof Number)
			return (Number)value;

		if(cls == int.class  || cls == long.class || cls == short.class || cls == byte.class)
		{
			if(value instanceof String)
				return Long.valueOf((String)value);

			if(value instanceof Boolean)
				return new Long(((Boolean)value).booleanValue() ? 1 : 0);
		}
		else if(cls == BigDecimal.class)
		{
			if(value instanceof String)
				return new BigDecimal((String)value);

			if(value instanceof Boolean)
				return new BigDecimal(((Boolean)value).booleanValue() ? 1 : 0);
		}
		if(cls == double.class  || cls == float.class)
		{
			if(value instanceof String)
				return Double.valueOf((String)value);

			if(value instanceof Boolean)
				return new Double(((Boolean)value).booleanValue() ? 1 : 0);
		}
		throw new SQLException("Cannot derive a Number from an object of class " + value.getClass().getName());
	}

	static Object basicCalendricalCoersion(Class cls, Object value, Calendar cal)
	throws SQLException
	{
		if(value == null)
			return value;

		if(cls.isInstance(value))
			return value;

		if(cls == Timestamp.class)
		{
			if(value instanceof Date)
			{
				cal.setTime((Date)value);
				cal.set(Calendar.HOUR_OF_DAY, 0);
				cal.set(Calendar.MINUTE, 0);
				cal.set(Calendar.SECOND, 0);
				cal.set(Calendar.MILLISECOND, 0);
				return new Timestamp(cal.getTimeInMillis());
			}
			else if(value instanceof Time)
			{
				cal.setTime((Date)value);
				cal.set(1970, 0, 1);
				return new Timestamp(cal.getTimeInMillis());
			}
			else if(value instanceof String)
			{
				return Timestamp.valueOf((String)value);
			}
		}
		else if(cls == Date.class)
		{
			if(value instanceof Timestamp)
			{
				Timestamp ts = (Timestamp)value;
				cal.setTime(ts);
				cal.set(Calendar.HOUR_OF_DAY, 0);
				cal.set(Calendar.MINUTE, 0);
				cal.set(Calendar.SECOND, 0);
				cal.set(Calendar.MILLISECOND, 0);
				return new Date(cal.getTimeInMillis());
			}
			else if(value instanceof String)
			{
				return Date.valueOf((String)value);
			}
		}
		else if(cls == Time.class)
		{
			if(value instanceof Timestamp)
			{
				Timestamp ts = (Timestamp)value;
				cal.setTime(ts);
				cal.set(1970, 0, 1);
				return new Time(cal.getTimeInMillis());
			}
			else if(value instanceof String)
			{
				return Time.valueOf((String)value);
			}
		}
		throw new SQLException("Cannot derive a value of class " +
			cls.getName() + " from an object of class " + value.getClass().getName());
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
	 * @notes: Subclasses should override this to support jdbc 4.0 support.
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
	 */
	public Object unwrap(Class iface)
	throws SQLException
	{
		throw new UnsupportedFeatureException("unwrap");
	}


    /*
     * This table holds the org.postgresql names for the types supported.
     * Any types that map to Types.OTHER (eg POINT) don't go into this table.
     * They default automatically to Types.OTHER
     *
     * Note: This must be in the same order as below.
     *
     * Tip: keep these grouped together by the Types. value
     */
    public static final String JDBC3_TYPE_NAMES[] = {
                "int2",
                "int4", "oid",
                "int8",
                "cash", "money",
                "numeric",
                "float4",
                "float8",
                "bpchar", "char", "char2", "char4", "char8", "char16",
                "varchar", "text", "name", "filename",
                "bytea",
                "bool",
                "bit",
                "date",
                "time", "timetz",
                "abstime", "timestamp", "timestamptz",
                "_bool", "_char", "_int2", "_int4", "_text",
                "_oid", "_varchar", "_int8", "_float4", "_float8",
                "_abstime", "_date", "_time", "_timestamp", "_numeric",
                "_bytea"
            };

    /*
     * This table holds the JDBC type for each entry above.
     *
     * Note: This must be in the same order as above
     *
     * Tip: keep these grouped together by the Types. value
     */
    public static final int JDBC_TYPE_NUMBERS[] =
    		{
                Types.SMALLINT,
                Types.INTEGER, Types.INTEGER,
                Types.BIGINT,
                Types.DOUBLE, Types.DOUBLE,
                Types.NUMERIC,
                Types.REAL,
                Types.DOUBLE,
                Types.CHAR, Types.CHAR, Types.CHAR, Types.CHAR, Types.CHAR, Types.CHAR,
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.BINARY,
                Types.BIT,
                Types.BIT,
                Types.DATE,
                Types.TIME, Types.TIME,
                Types.TIMESTAMP, Types.TIMESTAMP, Types.TIMESTAMP,
                Types.ARRAY, Types.ARRAY, Types.ARRAY, Types.ARRAY, Types.ARRAY,
                Types.ARRAY, Types.ARRAY, Types.ARRAY, Types.ARRAY, Types.ARRAY,
                Types.ARRAY, Types.ARRAY, Types.ARRAY, Types.ARRAY, Types.ARRAY,
                Types.ARRAY
            };
 }
