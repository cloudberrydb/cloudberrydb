/*-------------------------------------------------------------------------
 *
 * TestConnectorUtil
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

import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;

import static org.junit.Assert.assertEquals;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.io.IOException;

public class TestConnectorUtil  {
    private static MiniDFSCluster cluster;

    @BeforeClass
    public static void setupBeforeClass() throws Exception {
        // final Configuration conf = new Configuration();
        // cluster=  new MiniDFSCluster.Builder(conf).numDataNodes(2).format(true).build();
    }

    @AfterClass
    public static void teardownAfterClass() throws Exception {
        // cluster.shutdown();
    }

    @Test
    public void test_should_able_to_connect_to_hdfs() throws URISyntaxException {
        Configuration conf = new Configuration();
        URI inputURI = new URI("gphdfs://localhost:8020/test.txt");
    
        ConnectorUtil.setHadoopFSURI(conf, inputURI, "cdh4.1");

        assertEquals("hdfs://localhost:8020", conf.get("fs.defaultFS"));
    }

    @Test
    public void test_should_able_to_connect_to_hdfs_with_ha() throws URISyntaxException {
        Configuration conf = new Configuration();
        URI inputURI = new URI("gphdfs://nameservice1/test.txt");

        ConnectorUtil.setHadoopFSURI(conf, inputURI, "cdh4.1");

        assertEquals("hdfs://nameservice1", conf.get("fs.defaultFS"));
    }
}
