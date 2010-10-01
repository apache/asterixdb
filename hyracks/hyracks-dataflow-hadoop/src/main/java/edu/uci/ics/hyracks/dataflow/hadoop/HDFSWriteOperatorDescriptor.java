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
package edu.uci.ics.hyracks.dataflow.hadoop;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.hadoop.util.DatatypeHelper;
import edu.uci.ics.hyracks.dataflow.std.file.AbstractFileWriteOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.dataflow.std.file.IRecordWriter;
import edu.uci.ics.hyracks.dataflow.std.file.RecordWriter;



public class HDFSWriteOperatorDescriptor extends
		AbstractFileWriteOperatorDescriptor {
	
    private static String nullWritableClassName = NullWritable.class.getName();
	
    private static class HDFSWriter extends RecordWriter {
		
       HDFSWriter(FileSystem fileSystem, String hdfsPath, int[] columns, char separator)
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

    private static class HDFSSequenceWriter extends RecordWriter {
	private Writer writer;
	HDFSSequenceWriter(FileSystem fileSystem, String hdfsPath, Writer writer)
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
    Map<String,String> jobConfMap;
    

    @Override
    protected IRecordWriter createRecordWriter(File file,int index) throws Exception {
    	JobConf conf = DatatypeHelper.map2JobConf((HashMap)jobConfMap);
	System.out.println("replication:" + conf.get("dfs.replication"));
    	FileSystem fileSystem = null;
     	try {
	    fileSystem = FileSystem.get(conf);
	} catch (IOException ioe) {
            ioe.printStackTrace();
	}
	Path path = new Path(file.getAbsolutePath());
	checkIfCanWriteToHDFS(new FileSplit[]{new FileSplit("localhost",file)});
	FSDataOutputStream outputStream = fileSystem.create(path);
	outputStream.close();
	if(sequenceFileOutput) {
            Class keyClass = Class.forName(conf.getOutputKeyClass().getName());  
	    Class valueClass= Class.forName(conf.getOutputValueClass().getName());
            conf.setClass("mapred.output.compression.codec", GzipCodec.class, CompressionCodec.class);
   	    Writer writer = SequenceFile.createWriter(fileSystem, conf,path, keyClass, valueClass);
	    return new HDFSSequenceWriter(fileSystem, file.getAbsolutePath(), writer);
        } else {
	    return new HDFSWriter(fileSystem, file.getAbsolutePath(), null, COMMA);
        }	
    }
    
    private boolean checkIfCanWriteToHDFS(FileSplit[] fileSplits) throws Exception{
    	boolean canWrite = true;
    	JobConf conf = DatatypeHelper.map2JobConf((HashMap)jobConfMap);
	FileSystem fileSystem = null;
	try {
	    fileSystem = FileSystem.get(conf);
	    for(FileSplit fileSplit : fileSplits) {
		Path path = new Path(fileSplit.getLocalFile().getAbsolutePath());
		canWrite = !fileSystem.exists(path);
		if(!canWrite){
	            throw new Exception(" Output path :  already exists");
		}	
            }
	} catch(IOException ioe) {
	    ioe.printStackTrace();
	    throw ioe;
	}
        return canWrite;
   }    

    private static String getAbsolutePath(Path path) {
          StringBuffer absolutePath = new StringBuffer();
          List<String> ancestorPath = new ArrayList<String>();
          Path pathElement=path;
          while(pathElement != null) {
                ancestorPath.add(0, pathElement.getName());
             pathElement = pathElement.getParent();
          }
          ancestorPath.remove(0);        
          for(String s : ancestorPath) {
              absolutePath.append("/");
              absolutePath.append(s);
          }
          return new String(absolutePath);
     }

     private static FileSplit[] getOutputSplits(JobConf conf,int noOfMappers){
         int numOutputters = conf.getNumReduceTasks() != 0 ? conf.getNumReduceTasks() : noOfMappers;
         FileSplit[] outputFileSplits = new FileSplit[numOutputters];
         String absolutePath = getAbsolutePath(FileOutputFormat.getOutputPath(conf));
         for(int index=0;index<numOutputters;index++) {
             String suffix = new String("part-00000");
             suffix = new String(suffix.substring(0, suffix.length() - ("" + index).length()));
             suffix = suffix + index;
             String outputPath = absolutePath + "/" + suffix;
             outputFileSplits[index] = new FileSplit("localhost",new File(outputPath));
         }
         return outputFileSplits;
    }

    public HDFSWriteOperatorDescriptor(JobSpecification jobSpec,JobConf jobConf, int numMapTasks) throws Exception{
        super(jobSpec,getOutputSplits(jobConf,numMapTasks));
        this.jobConfMap = DatatypeHelper.jobConf2Map(jobConf);
        checkIfCanWriteToHDFS(super.splits);
	    this.sequenceFileOutput = 
	    (jobConf.getOutputFormat() instanceof SequenceFileOutputFormat);
    }
}
