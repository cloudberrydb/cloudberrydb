/*
 * Copyright (c) 2009 - Greenplum 
 */
package org.postgresql.pljava.sqlj;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.net.URL;
import java.net.MalformedURLException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.jar.Attributes;
import java.util.jar.Attributes.Name;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;

/**
 * @author Caleb Welton
 */
public class JarLoader extends ClassLoader
{
	private final URL m_url;
	private final Map m_entries;
	private Map       m_images;
	private Manifest  m_manifest;

	/**
	 * Creates a loader to load a specified JAR file.
	 *
	 * We place no restrictions on what the <code>URL</code> points to here.
	 * instead any access restrictions (from trusted java) are enforced by 
	 * the <code>Security Manager</code> defined in <code>Backend.java</code>.
	 *
	 * @param url : The URL to load
	 * @return JarLoader : Classloader for a specified JAR
	 * @throws IOException : when the Jar cannot be read
	 *
	 */
	JarLoader(ClassLoader parent, URL url) throws IOException
	{
		super(parent);

		m_url = url;
		m_entries = new HashMap();
		m_images  = new HashMap();

		// Just having a JAR in your CLASSPATH will cause us to load it,
		// which means you need security access to it.
		JarInputStream jis = new JarInputStream(url.openStream());
		ByteArrayOutputStream img = new ByteArrayOutputStream();
		byte[] buf = new byte[1024];
		JarEntry je;
		for (je = jis.getNextJarEntry();
			 je != null;
			 je = jis.getNextJarEntry())
		{
			if (je.isDirectory())
				continue;
			String entryName = je.getName();
			Attributes attr = je.getAttributes();
			int nBytes;
			
			img.reset();
			while ((nBytes = jis.read(buf)) > 0)
				img.write(buf, 0, nBytes);
			jis.closeEntry();
				
			m_entries.put(entryName, img.toByteArray());
		}
		m_manifest = jis.getManifest();
	}

	/**
	 * Finds the class with the specified name.  This method is invoked by the 
	 * inherited <code>loadClass</code> method after checking the parent class
	 * loader for the requested class.
	 *
	 * For security reasons it is necessary to disable loading new classes
	 * into the <code>java.</code> or <code>org.postgresql.pljava.</code>
	 * namespaces.  Otherwise it would be possible for a user to create a
	 * class in this namespace.  Allowing new classes in this namespace
	 * would allow access to package-protected variables, manipulation of
	 * which could lead to a subversion of existing security measure.  Any
	 * attempt to load classes from this namespace will result in a 
	 * <code>ClassNotFoundException</code>.
	 *
	 * @param name - the binary name of the class
	 * @return The resulting <code>Class</code> object
	 * @throws ClassNotFoundException - If the class could not be found
	 */
	protected Class findClass(final String name)
		throws ClassNotFoundException
	{
		/* Protect the namespace */
		if (name.startsWith("java.") ||
			name.startsWith("org.postgresql.pljava"))
		{
			throw new ClassNotFoundException(name); 
		}

		/* Look to see if we have already loaded the class */
		Class c = findLoadedClass(name);
		if (c != null)
			return c;

		/* Check for the class within our jar */
		String path = name.replace('.', '/').concat(".class");
		byte[] entryImg = (byte[]) m_entries.get(path);
		
		/* If not found, raise an exception */
		if (entryImg == null)
			throw new ClassNotFoundException(name);

		/* create a package for the class */
		definePackage(name);

		/* otherwise convert the image to a class and return it */
		return defineClass(name, entryImg, 0, entryImg.length);
	}

	protected URL findResource(String name)
	{
		/* Lookup the name */
		Object entryImg = m_entries.get(name);
		
		/* Return null when not found */
		if (entryImg == null)
			return null;

		try 
		{
			URL url = new URL("jar",
							  m_url.getHost(),
							  "file:" + m_url.getPath() + "!/" + name);
			return url;
		}
		catch (MalformedURLException e)
		{
			throw new RuntimeException(e);
		}
	}


    /**
     * Define the package information associated with a class.
	 */
    protected void definePackage(String className) 
	{
        int    classIndex  = className.lastIndexOf('.');
		String specTitle   = null;
		String specVendor  = null;
		String specVersion = null;
        String implTitle   = null;
        String implVendor  = null;
        String implVersion = null;
        String sealed      = null;
        URL    sealBase    = null;

		/* Quick return if the class has no package */
        if (classIndex == -1)
            return;

		/* Or if the package has already been defined */
        String packageName = className.substring(0, classIndex);
        if (getPackage(packageName) != null)
            return;

		/* Read package information from the manifest, if any */
        if (m_manifest != null) 
        {
			String     sectionName       = packageName.replace('.', '/') + "/";
			Attributes sectionAttributes = m_manifest.getAttributes(sectionName);
			Attributes mainAttributes    = m_manifest.getMainAttributes();
			
			if (sectionAttributes != null)
			{
				specTitle   = sectionAttributes.getValue(Name.SPECIFICATION_TITLE);
				specVendor  = sectionAttributes.getValue(Name.SPECIFICATION_VENDOR);
				specVersion = sectionAttributes.getValue(Name.SPECIFICATION_VERSION);
				implTitle   = sectionAttributes.getValue(Name.IMPLEMENTATION_TITLE);
				implVendor  = sectionAttributes.getValue(Name.IMPLEMENTATION_VENDOR);
				implVersion = sectionAttributes.getValue(Name.IMPLEMENTATION_VERSION);
				sealed      = sectionAttributes.getValue(Name.SEALED);
			}

			if (mainAttributes != null)
			{
				if (specTitle == null)
					specTitle = mainAttributes.getValue(Name.SPECIFICATION_TITLE);

				if (specVendor == null)
					specVendor = mainAttributes.getValue(Name.SPECIFICATION_VENDOR);

				if (specVersion == null)
					specVersion = mainAttributes.getValue(Name.SPECIFICATION_VERSION);

				if (implTitle == null)
					implTitle = mainAttributes.getValue(Name.IMPLEMENTATION_TITLE);

				if (implVendor == null)
					implVendor = mainAttributes.getValue(Name.IMPLEMENTATION_VENDOR);
            
				if (implVersion == null) 
					implVersion = mainAttributes.getValue(Name.IMPLEMENTATION_VERSION);
            
				if (sealed == null) 
					sealed = mainAttributes.getValue(Name.SEALED);
            }
		}

		/* If the package was defined as sealed then use the jar url for the base */
		if (sealed != null && sealed.equalsIgnoreCase("true"))
			sealBase = m_url;

		/* finally define the package and return */
		definePackage(packageName, 
					  specTitle, specVersion, specVendor, 
					  implTitle, implVersion, implVendor, 
					  sealBase);
	}

}
