/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root directory of this distribution or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.tasks;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.tools.ant.BuildException;

public class JarLoaderTask extends PLJavaTask
{
	private boolean m_deploy = true;
	private boolean m_reload = false;
	private File m_file;
	private String m_name;
	private String m_schema;

	public void execute()
	throws BuildException
	{
        if(m_file == null)
			throw new BuildException("file must be set", this.getLocation());
		if(m_name == null)
			throw new BuildException("name must be set", this.getLocation());

		InputStream in = null;
		byte[] image;
		try
		{
			int count;
			in = new FileInputStream(m_file);
			ByteArrayOutputStream bld = new ByteArrayOutputStream();
			byte[] buf = new byte[0x1000];
			while((count = in.read(buf)) > 0)
				bld.write(buf, 0, count);
			image = bld.toByteArray();
		}
		catch(IOException e)
		{
			throw new BuildException("Unable to read file " + m_file, e, this.getLocation());
		}
		finally
		{
			if(in != null)
				try { in.close(); } catch(IOException e) {}
		}

		ResultSet rs = null;
		PreparedStatement stmt = null;
		Connection conn = this.getConnection();
		try
		{
			stmt = conn.prepareStatement(m_reload
				? "SELECT sqlj.replace_jar(?, ?, ?)"
				: "SELECT sqlj.install_jar(?, ?, ?)");

			stmt.setBytes(1, image);
			stmt.setString(2, m_name);
			stmt.setBoolean(3, m_deploy);
			stmt.executeQuery().close();

			if(m_schema != null)
			{
				boolean found = false;
				stmt.close();
				stmt = null;
				stmt = conn.prepareStatement("SELECT sqlj.get_classpath(?)");
				stmt.setString(1, m_schema);
				rs = stmt.executeQuery();
				
				String path = null;
				if(!rs.next())
				{
					path = rs.getString(1);
					if(path != null)
					{
						String[] jars = path.split(":");
						int idx = jars.length;
						while(--idx >= 0)
							if(jars[idx].equals(m_name))
							{
								found = true;
								break;
							}
					}
				}
				rs.close();
				rs = null;

				if(!found)
				{
					if(path != null && path.length() > 0)
						path = path + ":" + m_name;
					else
						path = m_name;
					stmt.close();
					stmt = null;
					stmt = conn.prepareStatement("SELECT sqlj.set_classpath(?, ?)");
					stmt.setString(1, m_schema);
					stmt.setString(2, path);
					stmt.executeQuery().close();
				}
			}
			conn.commit();
		}
		catch(SQLException e)
		{
			try { conn.rollback(); } catch(SQLException e2) {}
			throw new BuildException("Unable to do install_jar: " + e.getMessage(), e, this.getLocation());
		}
		finally
		{
			if(rs != null)
				try { rs.close(); } catch(SQLException e) {}
			if(stmt != null)
				try { stmt.close(); } catch(SQLException e) {}
			try { conn.close(); } catch(SQLException ingore) {}
		}
	}

	public final boolean isDeploy()
	{
		return m_deploy;
	}
	

	public final void setDeploy(boolean deploy)
	{
		m_deploy = deploy;
	}
	

	public final File getFile()
	{
		return m_file;
	}
	

	public final void setFile(File file)
	{
		m_file = file;
	}
	

	public final String getName()
	{
		return m_name;
	}
	

	public final void setName(String name)
	{
		m_name = name;
	}

	public final boolean isReload()
	{
		return m_reload;
	}
	

	public final void setReload(boolean reload)
	{
		m_reload = reload;
	}
	

	public final String getSchema()
	{
		return m_schema;
	}
	

	public final void setSchema(String schema)
	{
		m_schema = schema;
	}
	
	
}
