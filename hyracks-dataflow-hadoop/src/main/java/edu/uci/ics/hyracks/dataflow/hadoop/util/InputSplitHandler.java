/*
 * Copyright 2009-2010 University of California, Irvine
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
package edu.uci.ics.hyracks.dataflow.hadoop.util;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;



public class InputSplitHandler {

	private static final int CURRENT_SPLIT_FILE_VERSION = 0;
    private static final byte[] SPLIT_FILE_HEADER = "SPL".getBytes();
    
    public static InputSplit[] getInputSplits(JobConf jobConf, Path splitFilePath) throws IOException {
    	RawSplit [] rawSplits = RawSplit.readSplitsFile(jobConf, splitFilePath);
    	InputSplit [] inputSplits = new InputSplit[rawSplits.length];
    	for(int i=0;i<rawSplits.length;i++){
    		inputSplits[i] = getInputSplit(jobConf, rawSplits[i]);
    	}
    	return inputSplits;
    }
    
    public static void writeSplitFile(InputSplit[] inputSplits, JobConf jobConf,Path splitFilePath)
                                             throws IOException {
    	RawSplit.writeSplits(inputSplits ,jobConf, splitFilePath);
    }
    
    private static InputSplit getInputSplit(JobConf jobConf, RawSplit rawSplit) throws IOException{
		InputSplit inputSplit = null;
		String splitClass = rawSplit.getClassName();
		BytesWritable split  = rawSplit.getBytes();  
		try {
		     inputSplit = (InputSplit) 
		     ReflectionUtils.newInstance(jobConf.getClassByName(splitClass), jobConf);
		} catch (ClassNotFoundException exp) {
		     IOException wrap = new IOException("Split class " + splitClass + 
		                                         " not found");
		     wrap.initCause(exp);
		     throw wrap;
	    }
		DataInputBuffer splitBuffer = new DataInputBuffer();
		splitBuffer.reset(split.getBytes(), 0, split.getLength());
		inputSplit.readFields(splitBuffer);
		return inputSplit;
	}
    
    protected static class RawSplit implements Writable {
	    private String splitClass;
	    private BytesWritable bytes = new BytesWritable();
	    private String[] locations;
	    long dataLength;

	    public void setBytes(byte[] data, int offset, int length) {
	      bytes.set(data, offset, length);
	    }

		public void setClassName(String className) {
		   splitClass = className;
		}
		      
		public String getClassName() {
		   return splitClass;
		}
		      
		public BytesWritable getBytes() {
		   return bytes;
		}

		public void clearBytes() {
		    bytes = null;
		}
		      
		public void setLocations(String[] locations) {
		   this.locations = locations;
		}
		      
		public String[] getLocations() {
		   return locations;
		}
		      
		public void readFields(DataInput in) throws IOException {
		   splitClass = Text.readString(in);
		   dataLength = in.readLong();
		   bytes.readFields(in);
		   int len = WritableUtils.readVInt(in);
		   locations = new String[len];
		   for(int i=0; i < len; ++i) {
		       locations[i] = Text.readString(in);
		   }
        }
		      
		public void write(DataOutput out) throws IOException {
		   Text.writeString(out, splitClass);
		   out.writeLong(dataLength);
		   bytes.write(out);
		   WritableUtils.writeVInt(out, locations.length);
		  for(int i = 0; i < locations.length; i++) {
		     Text.writeString(out, locations[i]);
		  }        
		}

        public long getDataLength() {
	       return dataLength;
	    }
	
        public void setDataLength(long l) {
		    dataLength = l;
		}
		    
		public static RawSplit[] readSplitsFile(JobConf conf, Path splitFilePath) throws IOException{
		    FileSystem fs = FileSystem.get(conf);
		   	DataInputStream splitFile =
		    fs.open(splitFilePath);
		    try {
		    	byte[] header = new byte[SPLIT_FILE_HEADER.length];
		    	splitFile.readFully(header);
		        if (!Arrays.equals(SPLIT_FILE_HEADER, header)) {
		          throw new IOException("Invalid header on split file");
		        }
		        int vers = WritableUtils.readVInt(splitFile);
		        if (vers != CURRENT_SPLIT_FILE_VERSION) {
		          throw new IOException("Unsupported split version " + vers);
		        }
		        int len = WritableUtils.readVInt(splitFile);
		        RawSplit[] result = new RawSplit[len];
		        for(int i=0; i < len; ++i) {
		          result[i] = new RawSplit();
		          result[i].readFields(splitFile);
		        }
		        return result;
		    	
		    } finally {
		      splitFile.close();
		    }
		   }
		   
		  public static int writeSplits(InputSplit[] splits, JobConf job, 
		            Path submitSplitFile) throws IOException {
			// sort the splits into order based on size, so that the biggest
			// go first
			Arrays.sort(splits, new Comparator<InputSplit>() {
				public int compare(InputSplit a, InputSplit b) {
				    try {
					   long left = a.getLength();	
						
					   long right = b.getLength();
					   if (left == right) {
						   return 0;
					   } else if (left < right) {
						   return 1;
					   } else {
						   return -1;
					   }
				    } catch (IOException ie) {
				    	throw new RuntimeException("Problem getting input split size",
				                     ie);
				    }
			    }
			});
			DataOutputStream out = writeSplitsFileHeader(job, submitSplitFile, splits.length);
			try {
				DataOutputBuffer buffer = new DataOutputBuffer();
				RawSplit rawSplit = new RawSplit();
				for(InputSplit split: splits) {
					rawSplit.setClassName(split.getClass().getName());
					buffer.reset();
				    split.write(buffer);
				    rawSplit.setDataLength(split.getLength());
				    rawSplit.setBytes(buffer.getData(), 0, buffer.getLength());
				    rawSplit.setLocations(split.getLocations());
				    rawSplit.write(out);
				}
			} finally {
			out.close();
			}
			return splits.length;
		}

		private static DataOutputStream writeSplitsFileHeader(Configuration conf,
		                                                      Path filename,
		                                                      int length
		                                                     ) throws IOException {
		   	FileSystem fs = filename.getFileSystem(conf);
		   	FSDataOutputStream out = fs.create(filename);
		   	out.write(SPLIT_FILE_HEADER);
		   	WritableUtils.writeVInt(out, CURRENT_SPLIT_FILE_VERSION);
		   	WritableUtils.writeVInt(out, length);
		   	return out;
	    }
	 }  

}
