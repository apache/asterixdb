/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.coreops.hadoop;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.JobConf;

import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.coreops.file.AbstractFileWriteOperatorDescriptor;
import edu.uci.ics.hyracks.coreops.file.FileSplit;
import edu.uci.ics.hyracks.coreops.file.IRecordWriter;
import edu.uci.ics.hyracks.coreops.file.RecordWriter;
import edu.uci.ics.hyracks.hadoop.util.DatatypeHelper;



public class HDFSWriteOperatorDescriptor extends
		AbstractFileWriteOperatorDescriptor {
	
	private static String nullWritableClassName = NullWritable.class.getName();
	
	private static class HDFSLineWriterImpl extends RecordWriter {
		
        HDFSLineWriterImpl(FileSystem fileSystem, String hdfsPath, int[] columns, char separator)
                throws Exception {
    		super(columns,separator,new Object[]{fileSystem,hdfsPath});
        }

		@Override
		public OutputStream createOutputStream(Object[] args) throws Exception {
			FSDataOutputStream fs = ((FileSystem)args[0]).create(new Path((String)args[1]));
			return fs;
		}

		 @Override
	     public void write(Object[] record) throws Exception {
	         if(!nullWritableClassName.equals((record[0].getClass().getName()))){
	             bufferedWriter.write(String.valueOf(record[0]));
	         }
	         if(!nullWritableClassName.equals((record[1].getClass().getName()))){
	        	  bufferedWriter.write(separator);	 
	        	  bufferedWriter.write(String.valueOf(record[1]));
	         }	 
	         bufferedWriter.write("\n");
	     }
    }

	private static class HDFSSequenceWriterImpl extends RecordWriter {
		
		private Writer writer;
		
		HDFSSequenceWriterImpl(FileSystem fileSystem, String hdfsPath, Writer writer)
                throws Exception {
    		super(null,COMMA,new Object[]{fileSystem,hdfsPath});
    		this.writer = writer;
        }

		@Override
		public OutputStream createOutputStream(Object[] args) throws Exception {
			return null;
		}
		
		@Override
	     public void close() {
	         try {
	             writer.close();
	         } catch (IOException e) {
	             e.printStackTrace();
	         }
	     }

	     @Override
	     public void write(Object[] record) throws Exception {
	         Object key = record[0];
	         Object value = record[1];
	         writer.append(key, value);
	     }

    }
	
    private static final long serialVersionUID = 1L;
    private static final char COMMA = ',';
	private char separator;
	private boolean sequenceFileOutput = false;
	private String keyClassName;
	private String valueClassName;
	Map<String,String> jobConfMap;
    

    @Override
    protected IRecordWriter createRecordWriter(File file,int index) throws Exception {
    	JobConf conf = DatatypeHelper.hashMap2JobConf((HashMap)jobConfMap);
		System.out.println("replication:" + conf.get("dfs.replication"));
    	FileSystem fileSystem = null;
		try{
			fileSystem = FileSystem.get(conf);
		}catch(IOException ioe){
			ioe.printStackTrace();
		}
		Path path = new Path(file.getAbsolutePath());
		checkIfCanWriteToHDFS(new FileSplit[]{new FileSplit("localhost",file)});
		FSDataOutputStream outputStream = fileSystem.create(path);
		outputStream.close();
		if(sequenceFileOutput){
			Class  keyClass = Class.forName(keyClassName);  
			Class valueClass= Class.forName(valueClassName);
			conf.setClass("mapred.output.compression.codec", GzipCodec.class, CompressionCodec.class);
			Writer writer = SequenceFile.createWriter(fileSystem, conf,path, keyClass, valueClass);
			return new HDFSSequenceWriterImpl(fileSystem, file.getAbsolutePath(), writer);
		}else{
			return new HDFSLineWriterImpl(fileSystem, file.getAbsolutePath(), null, COMMA);
		}	
    }
    
    private boolean checkIfCanWriteToHDFS(FileSplit[] fileSplits) throws Exception{
    	boolean canWrite = true;
    	JobConf conf = DatatypeHelper.hashMap2JobConf((HashMap)jobConfMap);
		FileSystem fileSystem = null;
		try{
			fileSystem = FileSystem.get(conf);
		    for(FileSplit fileSplit : fileSplits){
				Path path = new Path(fileSplit.getLocalFile().getAbsolutePath());
				canWrite = !fileSystem.exists(path);
				if(!canWrite){
					throw new Exception(" Output path :  already exists");
				}	
			}
		}catch(IOException ioe){
			ioe.printStackTrace();
			throw ioe;
		}
	    return canWrite;
    }
		
	
	public HDFSWriteOperatorDescriptor(JobSpecification jobSpec,Map<String,String> jobConfMap, FileSplit[] fileSplits,char seperator,boolean sequenceFileOutput,String keyClassName, String valueClassName) throws Exception{
		super(jobSpec,fileSplits);
		this.jobConfMap = jobConfMap;
		checkIfCanWriteToHDFS(fileSplits);
		this.separator = seperator;
		this.sequenceFileOutput = sequenceFileOutput;
		this.keyClassName = keyClassName;
		this.valueClassName = valueClassName;
	}
}
