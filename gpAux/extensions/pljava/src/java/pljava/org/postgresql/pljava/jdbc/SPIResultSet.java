/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.jdbc;

import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

import org.postgresql.pljava.internal.Portal;
import org.postgresql.pljava.internal.SPI;
import org.postgresql.pljava.internal.TupleTable;
import org.postgresql.pljava.internal.Tuple;
import org.postgresql.pljava.internal.TupleDesc;

/**
 * A Read-only ResultSet that provides direct access to a {@link
 * org.postgresql.pljava.internal.Portal Portal}. At present, only
 * forward positioning is implemented. Attempts to use reverse or
 * absolute positioning will fail.
 *
 * @author Thomas Hallgren
 */
public class SPIResultSet extends ResultSetBase
{
	private final SPIStatement m_statement;
	private final Portal    m_portal;
	private final TupleDesc m_tupleDesc;
	private final int       m_maxRows;

	private Tuple m_currentRow;
	private Tuple m_nextRow;

	private TupleTable m_table;
	private int m_tableRow;

	SPIResultSet(SPIStatement statement, Portal portal, int maxRows)
	throws SQLException
	{
		super(statement.getFetchSize());
		m_statement = statement;
		m_portal = portal;
		m_maxRows = maxRows;
		m_tupleDesc = portal.getTupleDesc();
		m_tableRow = -1;
	}

	public int getFetchDirection()
	throws SQLException
	{
		return FETCH_FORWARD;
	}

	public void close()
	throws SQLException
	{
		if(m_portal.isValid())
		{
			m_portal.close();
			m_statement.resultSetClosed(this);
			m_table      = null;
			m_tableRow   = -1;
			m_currentRow = null;
			m_nextRow    = null;
			super.close();
		}
	}

	public int getHoldability()
	throws SQLException
	{
		return ResultSet.HOLD_CURSORS_OVER_COMMIT;
	}

	public boolean isLast() throws SQLException
	{
		return m_currentRow != null && this.peekNext() == null;
	}

	public boolean next()
	throws SQLException
	{
		m_currentRow = this.peekNext();
		m_nextRow = null;
		boolean result = (m_currentRow != null);
		this.setRow(result ? this.getRow() + 1 : -1);
		return result;
	}

	public String getCursorName()
	throws SQLException
	{
		return this.getPortal().getName();
	}

	public int findColumn(String columnName)
	throws SQLException
	{
		return m_tupleDesc.getColumnIndex(columnName);
	}

	public Statement getStatement()
	throws SQLException
	{
		return m_statement;
	}

	protected final Portal getPortal()
	throws SQLException
	{
		if(!m_portal.isValid())
			throw new SQLException("ResultSet is closed");
		return m_portal;
	}

	protected final TupleTable getTupleTable()
	throws SQLException
	{
		if(m_table == null)
		{
			Portal portal = this.getPortal();
			if(portal.isAtEnd())
				return null;

			int mx;
			int fetchSize = this.getFetchSize();
			if(m_maxRows > 0)
			{
				mx = m_maxRows - portal.getPortalPos();
				if(mx <= 0)
					return null;
				if(mx > fetchSize)
					mx = fetchSize;
			}
			else
				mx = fetchSize;

			try
			{
				int result = portal.fetch(true, mx);
				if(result > 0)
					m_table = SPI.getTupTable(m_tupleDesc);
				m_tableRow = -1;
			}
			finally
			{
				SPI.freeTupTable();
			}
		}
		return m_table;
	}

	protected final Tuple getCurrentRow()
	throws SQLException
	{
		if(m_currentRow == null)
			throw new SQLException("ResultSet is not positioned on a valid row");
		return m_currentRow;
	}

	protected final Tuple peekNext()
	throws SQLException
	{
		if(m_nextRow != null)
			return m_nextRow;

		TupleTable table = this.getTupleTable();
		if(table == null)
			return null;

		if(m_tableRow >= table.getCount() - 1)
		{
			// Current table is exhaused, get the next
			// one.
			//
			m_table = null;
			table = this.getTupleTable();
			if(table == null)
				return null;
		}
		m_nextRow = table.getSlot(++m_tableRow);
		return m_nextRow;
	}

	protected Object getObjectValue(int columnIndex)
	throws SQLException
	{
		return this.getCurrentRow().getObject(m_tupleDesc, columnIndex);
	}

	public ResultSetMetaData getMetaData()
	throws SQLException
	{
		return new SPIResultSetMetaData(m_tupleDesc);
	}
}
