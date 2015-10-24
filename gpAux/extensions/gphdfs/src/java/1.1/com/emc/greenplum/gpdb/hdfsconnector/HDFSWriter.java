/*-------------------------------------------------------------------------
 *
 * HDFSWritaber.java
 *
 * Copyright (c) 2011, Greenplum inc
 *
 *-------------------------------------------------------------------------
 */


package com.emc.greenplum.gpdb.hdfsconnector;

import com.emc.greenplum.gpdb.hadoop.io.GPDBWritable;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSWriter
{
    protected Configuration conf;
	protected int           segid;
	protected boolean       isTextFormat;
	
	protected String        xid;
	protected String        outputpath;
	protected TaskAttemptContext fakeTaskCtx;

	protected boolean isCompressed = false;
	protected String  compressType = null;
	protected String  compressCodec = null;
	
	private InputStream instream;
    
	/**
	 * Constructor - parse the input args
	 * 
 	 * @param args the list of input argument, that must be
	 * local segment id, xid, format (TEXT or GPDBWritable)
	 * and the location in the form of gphdfs://host:port/file_path
	 * 
	 * @throws Exception when something goes wrong

	 */
	public HDFSWriter(String[] args) {
	    /** 
	     * The following input argments are required:
	     * arg[0] : local segment id
         * arg[1] : xid
	     * arg[2] : format: TEXT or GPDBWritable
	     * arg[3] : hadoop connector version
	     * arg[4] : gphdfs://host:port/file_path?<query>
	     * query contains:
	     *  compress: true or false
	     * 	compression_type (for gpdbwritable only): BLOCK and RECORD (gpdbwritable)
	     *  codec (for gpdbwritable only): codec class name
	     */
	    if (args.length != 5)
	        throw new IllegalArgumentException("Wrong number of argument");

		segid         = Integer.parseInt(args[0]);
		xid           = args[1]; 
		isTextFormat  = (args[2].equals("TEXT"));
		String connVer = args[3];
	    URI outputURI;
		try {
			outputURI = new URI(args[4]);
		} catch (URISyntaxException e) {
			throw new IllegalArgumentException("Illegal input uri: "+args[4]);
		}

		/**
		 * Initialize conf, a fake ctx and compression var
		 * Using a fake context is ok because only the confg portion
		 * is used.
		 */
	    //Configuration.addDefaultResource("hdfs-site.xml");//items in hdfs-site.xml
		//will be overlapped by the same named item in hdfs-default.xml
	    conf        = new Configuration();
		conf.addResource("hdfs-site.xml");//this line is not needed now, but for line 87, 
		//it may related with some unknow old hadoop version, so I add it here
	    fakeTaskCtx = new TaskAttemptContext(conf, new TaskAttemptID());
	    isCompressed = FileOutputFormat.getCompressOutput(fakeTaskCtx);
	    compressType = conf.get("mapred.output.compression.type",
	    		SequenceFile.CompressionType.RECORD.toString());
	    compressCodec = conf.get("mapred.output.compression.codec");
		
	    // Set the HDFS URI
	    ConnectorUtil.setHadoopFSURI(conf, outputURI, connVer);
	    
	    // Setup the output path to <path>/seg_id_xid
	    outputpath = outputURI.getPath()+Path.SEPARATOR+segid+"_"+xid;

	    // Parse the compression type and codec from the URI query
	    if (outputURI.getQuery() != null) {
		    String[] kv_pair = outputURI.getQuery().split("&");
		    for(int i=0; i<kv_pair.length; i++) {
		    	String[] kv = kv_pair[i].split("=");
		    	String key = kv[0].toLowerCase();
		    	String val = kv[1];
		    	
		    	if (key.equals("compress"))
		    		isCompressed  = val.toLowerCase().equals("true");
		    	else if (key.equals("compression_type"))
		    		compressType  = val;
		    	else if (key.equals("codec"))
		    		compressCodec = val;
		    	else
		    		throw new IllegalArgumentException("Unexpected parameter:"+key);
		    }
	    }

	    instream = System.in;
	}


	/** 
	 * Either TEXT or GPDBWritable. Call the appropiate write method
	 */
	public void doWrite() throws IOException, InterruptedException {
		ConnectorUtil.loginSecureHadoop(conf);

		if (isTextFormat)
			writeTextFormat();
		else
			writeGPDBWritableFormat();
	}
	
	/**
	 * Helper routine to get the compression codec class
	 */
	protected Class<? extends CompressionCodec> getCodecClass(
			String name, Class<? extends CompressionCodec> defaultValue) {
		Class<? extends CompressionCodec> codecClass = defaultValue;
	    if (name != null) {
	    	try {
	    		codecClass = conf.getClassByName(name).asSubclass(CompressionCodec.class);
	    	} catch (ClassNotFoundException e) {
	    		throw new IllegalArgumentException("Compression codec " + name + " was not found.", e);
	    	}
	    }
	    return codecClass;
	}

	
	/**
	 * Write as GPDBOutputFormat, which is SequenceFile <LongWritable, GPDBWritable>
	 */
	protected void writeGPDBWritableFormat() throws IOException, InterruptedException {
		RecordWriter<LongWritable, GPDBWritable> rw = getGPDBRecordWriter();
		LongWritable segIDWritable = new LongWritable(segid);
		
		DataInputStream dis = new DataInputStream(instream);
		
		// Read from Std-in to construct "value"
		try {
			while(true) {
				GPDBWritable gw = new GPDBWritable();
				gw.readFields(dis);
				rw.write(segIDWritable, gw);
			}
		} catch (EOFException e) {}
		
		rw.close(fakeTaskCtx);
	}
	
	/**
	 * Write as TextOutputFormat
	 */
	protected void writeTextFormat() throws IOException {
		// construct the output stream
		DataOutputStream dos;
	    FileSystem fs = FileSystem.get(conf);

		// Open the output file based on compression type and codec
	    if (!isCompressed) {
		    Path file = new Path(outputpath);
	    	dos = fs.create(file, false);
	    }
	    else {
	        CompressionCodec codec = 
	        	(CompressionCodec) ReflectionUtils.newInstance(
	        			getCodecClass(compressCodec, GzipCodec.class),
	        			conf);
	        String extension = codec.getDefaultExtension();
	        
	        Path file = new Path(outputpath+extension);
	        FSDataOutputStream fileOut = fs.create(file, false);
	    	dos = new DataOutputStream(codec.createOutputStream(fileOut));
	    }
		
	    // read bytes from std-in and write it to the outputstream
	    byte[] buf = new byte[1024];
	    int  read;
	    while((read=instream.read(buf)) > 0)
	      dos.write(buf,0,read);

	    dos.close();
	}
		
	/**
	 * Helper function to construct the SequenceFile writer
	 */
	private SequenceFile.Writer getSequenceFileWriter() throws IOException {
		// Get the compression type and codec first
	    CompressionCodec codec = null;
	    SequenceFile.CompressionType compressionType = SequenceFile.CompressionType.NONE;
	    if (isCompressed) {
	    	compressionType = SequenceFile.CompressionType.valueOf(compressType);
	    	codec = (CompressionCodec) ReflectionUtils.newInstance(
	    			getCodecClass(compressCodec, DefaultCodec.class),
	    			conf);
	    }
	    
	    // get the path of the  output file 
	    Path file = new Path(outputpath);
	    FileSystem fs = file.getFileSystem(conf);
	    return SequenceFile.createWriter(fs, conf, file,
	             LongWritable.class,
	             GPDBWritable.class,
	             compressionType,
	             codec,
	             fakeTaskCtx);
	}
	
	/**
	 * Helper function to construct the SequenceFile writer
	 */
	private RecordWriter<LongWritable, GPDBWritable> getGPDBRecordWriter() throws IOException {
		final SequenceFile.Writer out = getSequenceFileWriter();
		
		return new RecordWriter<LongWritable, GPDBWritable>() {
			public void write(LongWritable key, GPDBWritable value)
			throws IOException {
				out.append(key, value);
			}
			
			public void close(TaskAttemptContext context) throws IOException { 
				out.close();
			}
		};
	}


	public static void main(String[] args) throws Exception
	{
		HDFSWriter hw = new HDFSWriter(args);
		hw.doWrite();
	} 	

}
