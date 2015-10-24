/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.jdbc;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.Calendar;

import org.postgresql.pljava.internal.ExecutionPlan;
import org.postgresql.pljava.internal.Oid;

/**
 *
 * @author Thomas Hallgren
 */
public class SPIPreparedStatement extends SPIStatement implements PreparedStatement
{
	private final Oid[]    m_typeIds;
	private final Object[] m_values;
	private final int[]    m_sqlTypes;
	private final String   m_statement;
	private ExecutionPlan  m_plan;

	public SPIPreparedStatement(SPIConnection conn, String statement, int paramCount)
	{
		super(conn);
		m_statement = statement;
		m_typeIds   = new Oid[paramCount];
		m_values    = new Object[paramCount];
		m_sqlTypes  = new int[paramCount];
		Arrays.fill(m_sqlTypes, Types.NULL);
	}

	public void close()
	throws SQLException
	{
		if(m_plan != null)
		{
			m_plan.close();
			m_plan = null;
		}
		this.clearParameters();
		super.close();
		Invocation.current().forgetStatement(this);
	}

	public ResultSet executeQuery()
	throws SQLException
	{
		this.execute();
		return this.getResultSet();
	}

	public int executeUpdate()
	throws SQLException
	{
		this.execute();
		return this.getUpdateCount();
	}

	public void setNull(int columnIndex, int sqlType)
	throws SQLException
	{
		this.setObject(columnIndex, null, sqlType);
	}

	public void setBoolean(int columnIndex, boolean value) throws SQLException
	{
		this.setObject(columnIndex, value ? Boolean.TRUE : Boolean.FALSE, Types.BOOLEAN);
	}

	public void setByte(int columnIndex, byte value) throws SQLException
	{
		this.setObject(columnIndex, new Byte(value), Types.TINYINT);
	}

	public void setShort(int columnIndex, short value) throws SQLException
	{
		this.setObject(columnIndex, new Short(value), Types.SMALLINT);
	}

	public void setInt(int columnIndex, int value) throws SQLException
	{
		this.setObject(columnIndex, new Integer(value), Types.INTEGER);
	}

	public void setLong(int columnIndex, long value) throws SQLException
	{
		this.setObject(columnIndex, new Long(value), Types.BIGINT);
	}

	public void setFloat(int columnIndex, float value) throws SQLException
	{
		this.setObject(columnIndex, new Float(value), Types.FLOAT);
	}

	public void setDouble(int columnIndex, double value) throws SQLException
	{
		this.setObject(columnIndex, new Double(value), Types.DOUBLE);
	}

	public void setBigDecimal(int columnIndex, BigDecimal value) throws SQLException
	{
		this.setObject(columnIndex, value, Types.DECIMAL);
	}

	public void setString(int columnIndex, String value) throws SQLException
	{
		this.setObject(columnIndex, value, Types.VARCHAR);
	}

	public void setBytes(int columnIndex, byte[] value) throws SQLException
	{
		this.setObject(columnIndex, value, Types.VARBINARY);
	}

	public void setDate(int columnIndex, Date value) throws SQLException
	{
		this.setObject(columnIndex, value, Types.DATE);
	}

	public void setTime(int columnIndex, Time value) throws SQLException
	{
		this.setObject(columnIndex, value, Types.TIME);
	}

	public void setTimestamp(int columnIndex, Timestamp value) throws SQLException
	{
		this.setObject(columnIndex, value, Types.TIMESTAMP);
	}

	public void setAsciiStream(int columnIndex, InputStream x)
	throws SQLException
	{
		try
		{
			Reader inputReader = new InputStreamReader(x, "US-ASCII");
			Clob   clob        = new ClobValue(inputReader);
			this.setObject(columnIndex, clob, Types.CLOB);
		}
		catch(UnsupportedEncodingException e)
		{
			throw new SQLException("US-ASCII encoding is not supported by this JVM");
		}

	}

	public void setAsciiStream(int columnIndex, InputStream x, int length) 
	throws SQLException
	{
		try
		{
			Reader inputReader = new InputStreamReader(x, "US-ASCII");
			Clob   clob        = new ClobValue(inputReader, length);
			this.setObject(columnIndex, clob, Types.CLOB);
		}
		catch(UnsupportedEncodingException e)
		{
			throw new SQLException("US-ASCII encoding is not supported by this JVM");
		}
	}

	public void setAsciiStream(int columnIndex, InputStream x, long length) 
	throws SQLException
	{
		try
		{
			Reader inputReader = new InputStreamReader(x, "US-ASCII");
			Clob   clob        = new ClobValue(inputReader, length);
			this.setObject(columnIndex, clob, Types.CLOB);
		}
		catch(UnsupportedEncodingException e)
		{
			throw new SQLException("US-ASCII encoding is not supported by this JVM");
		}
	}

	/**
	 * @deprecated
	 */
	public void setUnicodeStream(int columnIndex, InputStream value, int arg2) 
    throws SQLException
	{
		throw new UnsupportedFeatureException("PreparedStatement.setUnicodeStream");
	}

	public void setBinaryStream(int columnIndex, InputStream value) 
    throws SQLException
	{
		this.setObject(columnIndex, new BlobValue(value), Types.BLOB);
	}

	public void setBinaryStream(int columnIndex, InputStream value, int length) 
    throws SQLException
	{
		this.setObject(columnIndex, new BlobValue(value, length), Types.BLOB);
	}

	public void setBinaryStream(int columnIndex, InputStream value, long length) 
    throws SQLException
	{
		this.setObject(columnIndex, new BlobValue(value, length), Types.BLOB);
	}

	public void clearParameters()
	throws SQLException
	{
		Arrays.fill(m_values,   null);
		Arrays.fill(m_sqlTypes, Types.NULL);
	}

	public void setObject(int columnIndex, Object value, int sqlType, int scale)
	throws SQLException
	{
		this.setObject(columnIndex, value, sqlType);
	}

	public void setObject(int columnIndex, Object value, int sqlType)
	throws SQLException
	{
		if(columnIndex < 1 || columnIndex > m_sqlTypes.length)
			throw new SQLException("Illegal parameter index");

		Oid id = (sqlType == Types.OTHER)
			? Oid.forJavaClass(value.getClass())
			: Oid.forSqlType(sqlType);

		// Default to String.
		//
		if(id == null)
			id = Oid.forSqlType(Types.VARCHAR);

		Oid op = m_typeIds[--columnIndex];
		if(op == null)
			m_typeIds[columnIndex] = id;
		else if(!op.equals(id))
		{
			m_typeIds[columnIndex] = id;
			
			// We must re-prepare
			//
			if(m_plan != null)
				m_plan.close();
			m_plan = null;
		}
		m_sqlTypes[columnIndex] = sqlType;
		m_values[columnIndex] = value;
	}

	public void setObject(int columnIndex, Object value)
	throws SQLException
	{
		if(value == null)
			throw new SQLException("Can't assign null unless the SQL type is known");

		this.setObject(columnIndex, value, SPIConnection.getTypeForClass(value.getClass()));
	}

	/**
	 * Obtains the XOPEN SQL types for the parameters. 
	 * @return The array of types.
	 */
	private int[] getSqlTypes()
	{
		int   idx   = m_sqlTypes.length;
		int[] types = (int[])m_sqlTypes.clone();
		while(--idx >= 0)
		{
			if(types[idx] == Types.NULL)
				types[idx] = Types.VARCHAR;	// Default.
		}
		return types;
	}

	public boolean execute()
	throws SQLException
	{
		int[] sqlTypes = m_sqlTypes;
		int idx = sqlTypes.length;
		while(--idx >= 0)
			if(sqlTypes[idx] == Types.NULL)
				throw new SQLException("Not all parameters have been set");

		if(m_plan == null)
			m_plan = ExecutionPlan.prepare(m_statement, m_typeIds);

		boolean result = this.executePlan(m_plan, m_values);
		this.clearParameters(); // Parameters are cleared upon successful completion.
		return result;
	}

	/**
	 * The prepared statement cannot be used for executing oter statements.
	 * @throws SQLException indicating that this feature is not supported.
	 */
	public boolean execute(String statement)
	throws SQLException
	{
		throw new UnsupportedFeatureException("Can't execute other statements using a prepared statement");
	}

	public void addBatch()
	throws SQLException
	{
		this.internalAddBatch(new Object[]{m_values.clone(), m_sqlTypes.clone(), m_typeIds.clone()});
		this.clearParameters(); // Parameters are cleared upon successful completion.
	}

	/**
	 * The prepared statement cannot have other statements added too it.
	 * @throws SQLException indicating that this feature is not supported.
	 */
	public void addBatch(String statement)
	throws SQLException
	{
		throw new UnsupportedFeatureException("Can't add batch statements to a prepared statement");
	}

	public void setCharacterStream(int columnIndex, Reader value)
	throws SQLException
	{
		this.setObject(columnIndex, new ClobValue(value), Types.CLOB);
	}

	public void setCharacterStream(int columnIndex, Reader value, long length)
	throws SQLException
	{
		this.setObject(columnIndex, new ClobValue(value, length), Types.CLOB);
	}

	public void setCharacterStream(int columnIndex, Reader value, int length)
	throws SQLException
	{
		this.setObject(columnIndex, new ClobValue(value, length), Types.CLOB);
	}

	public void setRef(int columnIndex, Ref value) throws SQLException
	{
		this.setObject(columnIndex, value, Types.REF);
	}

	public void setBlob(int columnIndex, Blob value) throws SQLException
	{
		this.setObject(columnIndex, value, Types.BLOB);
	}

	public void setBlob(int columnIndex, InputStream value) 
	throws SQLException
	{
		this.setObject(columnIndex, new BlobValue(value), Types.BLOB);
	}

	public void setBlob(int columnIndex, InputStream value, long length)
	throws SQLException
	{
		this.setObject(columnIndex, new BlobValue(value, length), Types.BLOB);
	}

	public void setClob(int columnIndex, Clob value) throws SQLException
	{
		this.setObject(columnIndex, value, Types.CLOB);
	}

	public void setClob(int columnIndex, Reader value) 
	throws SQLException
	{
		this.setObject(columnIndex, new ClobValue(value), Types.CLOB);
	}

	public void setClob(int columnIndex, Reader value, long length)
	throws SQLException
	{
		this.setObject(columnIndex, new ClobValue(value, length), Types.CLOB);
	}

	public void setArray(int columnIndex, Array value) throws SQLException
	{
		this.setObject(columnIndex, value, Types.ARRAY);
	}

	public void setNCharacterStream(int columnIndex, Reader value)
	throws SQLException
	{
		throw new UnsupportedFeatureException("NClob data is not yet implemented");
	}

	public void setNCharacterStream(int columnIndex, Reader value, long length)
	throws SQLException
	{
		throw new UnsupportedFeatureException("NClob data is not yet implemented");
	}

	public void setNClob(int columnIndex, java.sql.NClob value) 
    throws SQLException
	{
		throw new UnsupportedFeatureException("NClob data is not yet implemented");
	}

	public void setNClob(int columnIndex, Reader value) 
	throws SQLException
	{
		throw new UnsupportedFeatureException("NClob data is not yet implemented");
	}

	public void setNClob(int columnIndex, Reader value, long length)
	throws SQLException
	{
		throw new UnsupportedFeatureException("NClob data is not yet implemented");
	}

	public void setNString(int columnIndex, String value)
	throws SQLException
	{
		throw new UnsupportedFeatureException("NString data is not yet implemented");
	}	

	public void setRowId(int columnIndex, java.sql.RowId value)
	throws SQLException
	{
		throw new UnsupportedFeatureException("RowId data is not yet implemented");
	}	

	public void setSQLXML(int columnIndex, java.sql.SQLXML value)
	throws SQLException
	{
		throw new UnsupportedFeatureException("SQLXML data is not yet implemented");
	}	


	/**
	 * ResultSetMetaData is not yet supported.
	 * @throws SQLException indicating that this feature is not supported.
	 */
	public ResultSetMetaData getMetaData()
	throws SQLException
	{
		throw new UnsupportedFeatureException("ResultSet meta data is not yet implemented");
	}

	public void setDate(int columnIndex, Date value, Calendar cal)
	throws SQLException
	{
		if(cal == null || cal == Calendar.getInstance())
			this.setObject(columnIndex, value, Types.DATE);
		throw new UnsupportedFeatureException("Setting date using explicit Calendar");
	}

	public void setTime(int columnIndex, Time value, Calendar cal)
	throws SQLException
	{
		if(cal == null || cal == Calendar.getInstance())
			this.setObject(columnIndex, value, Types.TIME);
		throw new UnsupportedFeatureException("Setting time using explicit Calendar");
	}

	public void setTimestamp(int columnIndex, Timestamp value, Calendar cal)
	throws SQLException
	{
		if(cal == null || cal == Calendar.getInstance())
			this.setObject(columnIndex, value, Types.TIMESTAMP);
		throw new UnsupportedFeatureException("Setting time using explicit Calendar");
	}

	public void setNull(int columnIndex, int sqlType, String typeName)
	throws SQLException
	{
		this.setNull(columnIndex, sqlType);
	}

	public void setURL(int columnIndex, URL value) throws SQLException
	{
		this.setObject(columnIndex, value, Types.DATALINK);
	}

	public String toString()
	{
		return m_statement;
	}

	/**
	 * Due to the design of the <code>SPI_prepare</code>, it is currently impossible to
	 * obtain the correct parameter meta data before all the parameters have been
	 * set, hence a ParameterMetaData obtained prior to setting the paramteres
	 * will have all parameters set to the default type {@link Types#VARCHAR}.
	 * Once the parameters have been set, a fair attempt is made to generate this
	 * object based on the supplied values.
	 * @return The meta data for parameter values.
	 */
	public ParameterMetaData getParameterMetaData()
	throws SQLException
	{
		return new SPIParameterMetaData(this.getSqlTypes());
	}

	protected int executeBatchEntry(Object batchEntry)
	throws SQLException
	{
		int ret = SUCCESS_NO_INFO;
		Object batchParams[] = (Object[])batchEntry;
		Object batchValues = batchParams[0];
		Object batchSqlTypes = batchParams[1];
		Object batchTypeIds[] = (Object[])batchParams[2];

		System.arraycopy(batchValues, 0, m_values, 0, m_values.length);
		System.arraycopy(batchSqlTypes, 0, m_sqlTypes, 0, m_sqlTypes.length);

		// Determine if we need to replan the query because the
		// types have changed from the last execution.
		//
		for (int i=0; i<m_typeIds.length; i++) {
			if (m_typeIds[i] != batchTypeIds[i]) {
				// We must re-prepare
				//
				if(m_plan != null) {
					m_plan.close();
					m_plan = null;
				}

				System.arraycopy(batchTypeIds, 0, m_typeIds, 0, m_typeIds.length);
				break;
			}
		}

		if(this.execute())
			this.getResultSet().close();
		else
		{
			int updCount = this.getUpdateCount();
			if(updCount >= 0)
				ret = updCount;
		}
		return ret;
	}
}
