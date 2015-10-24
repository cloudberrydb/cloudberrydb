/*-------------------------------------------------------------------------
 *
 * ConnectorUtil
 *
 * Copyright (c) 2011 EMC Corporation All Rights Reserved
 *
 * This software is protected, without limitation, by copyright law
 * and international treaties. Use of this software and the intellectual
 * property contained therein is expressly limited to the terms and
 * conditions of the License Agreement under which it is provided by
 * or on behalf of EMC.
 *
 *-------------------------------------------------------------------------
 */

package com.emc.greenplum.gpdb.hdfsconnector;

import com.emc.greenplum.gpdb.hadoop.io.GPDBWritable;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.io.*;

import java.io.*;
import java.io.DataOutputStream;
import java.util.*;
import java.net.URI;
import java.net.URISyntaxException;


/**
 * This is a common utility for the GPDB side Hadoop connector. Routines common to
 * HDFSReader and HDFSWriter would appear here.
 * Therefore, most of the routine should be protected.
 * @author achoi
 *
 */
public class ConnectorUtil
{
	/**
	 * Helper routine to translate the External table URI to the actual
	 * Hadoop fs.default.name and set it in the conf.
	 * MapR's URI starts with maprfs:///
	 * Hadoop's URI starts with hdfs://
	 * 
	 * @param conf the configuration
	 * @param inputURI the external table URI
	 * @param connVer the Hadoop Connector version
	 */
	protected static void setHadoopFSURI(Configuration conf, URI inputURI, String connVer)
	{
		/**
		 * Parse the URI and reconstruct the destination URI
		 * Scheme could be hdfs or maprfs
		 * 
		 * NOTE: Unless the version is MR, we're going to use ordinary
		 * hdfs://.
		 */
		if (connVer.startsWith("gpmr")) {
			conf.set("fs.maprfs.impl", "com.mapr.fs.MapRFileSystem");
			conf.set("fs.default.name", "maprfs:///" + inputURI.getHost());
		} else {
			String uri = inputURI.getHost();
			if (inputURI.getPort() != -1) {
				uri += ":"+inputURI.getPort();
			}

			conf.set("fs.default.name", "hdfs://" + uri);
		}
	}
}
