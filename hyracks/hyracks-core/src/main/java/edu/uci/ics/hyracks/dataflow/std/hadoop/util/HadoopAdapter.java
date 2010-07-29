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
package edu.uci.ics.hyracks.dataflow.std.hadoop.util;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.StringUtils;

import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;

public class HadoopAdapter {

	private static ClientProtocol namenode;
	private static FileSystem fileSystem;
	private static JobConf jobConf;
	private static HadoopAdapter instance;
	
	public static final String DFS_DATA_DIR = "dfs.data.dir";
	public static final String FS_DEFAULT_NAME = "fs.default.name";
	public static final String DFS_REPLICATION = "dfs.replication";
	
	public static HadoopAdapter getInstance(String fileSystemURL){
		if(instance == null){
			jobConf = new JobConf(true);
			String [] urlParts = parseURL(fileSystemURL);
			jobConf.set(FS_DEFAULT_NAME, fileSystemURL);
			instance = new HadoopAdapter(new InetSocketAddress(urlParts[1], Integer.parseInt(urlParts[2])));
		}
		return instance;
	}
	
	public static JobConf getConf() {
		return jobConf;
	}

	private HadoopAdapter (InetSocketAddress address){
		try{
			this.namenode = getNameNode(address);
			fileSystem = FileSystem.get(jobConf);
		}catch(IOException ioe){
			ioe.printStackTrace();
		}
	}
	
	private static String[] parseURL(String urlString){
		String[] urlComponents = urlString.split(":");
		urlComponents[1] = urlComponents[1].substring(2);
		return urlComponents;
	}
	
	
	public Map<String,List<HadoopFileSplit>> getInputSplits(String[] inputPaths){
	    List<HadoopFileSplit> hadoopFileSplits = new ArrayList<HadoopFileSplit>();
    	Path[] paths = new Path[inputPaths.length];
    	int index =0;
    	for(String inputPath : inputPaths){
    		paths[index++] = new Path(StringUtils.unEscapeString(inputPaths[0]));
    	}
    	Map<String,List<HadoopFileSplit>> fileSplitInfo = getBlocksToRead(paths);
    	return fileSplitInfo;
	}
	
	private static Map<String,List<HadoopFileSplit>> getBlocksToRead(Path[] inputPaths){
		Map<String,List<HadoopFileSplit>> hadoopFileSplitMap = new HashMap<String,List<HadoopFileSplit>>();
		for (int i=0;i<inputPaths.length;i++){
			try{
				String absolutePathPrint = getAbsolutePath(inputPaths[i]);
				FileStatus[] fileStatuses = namenode.getListing(absolutePathPrint);
				for(int j=0;j<fileStatuses.length;j++){
			    	Path filePath = fileStatuses[j].getPath();
			    	String absolutePath = getAbsolutePath(filePath);
			    	List<HadoopFileSplit> fileSplits = getFileBlocks(absolutePath,fileStatuses[j]);
			    	if(fileSplits!=null && fileSplits.size() > 0){
			    		hadoopFileSplitMap.put(absolutePath, fileSplits);
			    	}	
			    }		
			   }catch(IOException ioe){
				ioe.printStackTrace();
		    }
			
		}
		return hadoopFileSplitMap;
	}
	
	private static ClientProtocol getNameNode(InetSocketAddress address) throws IOException{
		return (ClientProtocol)getProtocol(ClientProtocol.class, address, new JobConf());
	}
	
	private static String getAbsolutePath(Path path){
		StringBuffer absolutePath = new StringBuffer();
		List<String> ancestorPath = new ArrayList<String>();
		Path pathElement=path;
		while(pathElement != null){
			ancestorPath.add(0, pathElement.getName());
			pathElement = pathElement.getParent();
		}
		ancestorPath.remove(0);
		for(String s : ancestorPath){
			absolutePath.append("/");
			absolutePath.append(s);
		}
		return new String(absolutePath);
	}
	
	private static VersionedProtocol getProtocol(Class protocolClass, InetSocketAddress inetAddress, JobConf jobConf) throws IOException{
		VersionedProtocol versionedProtocol = RPC.getProxy(protocolClass, ClientProtocol.versionID, inetAddress, jobConf);	
		return versionedProtocol;
	}
	
	private static List<HadoopFileSplit> getFileBlocks(String absolutePath,FileStatus fileStatus){
		List<HadoopFileSplit> hadoopFileSplits = new ArrayList<HadoopFileSplit>();
		try{
			LocatedBlocks locatedBlocks = namenode.getBlockLocations(absolutePath, 0, fileStatus.getLen());
			long blockSize = fileSystem.getBlockSize(new Path(absolutePath));
			if(locatedBlocks !=null){
	    		int index = 0;
				for(LocatedBlock locatedBlock : locatedBlocks.getLocatedBlocks()){
					DatanodeInfo[] datanodeInfos = locatedBlock.getLocations(); // all datanodes having this block
					String [] hostnames = new String[datanodeInfos.length];
					int datanodeCount =0;
					for(DatanodeInfo datanodeInfo : datanodeInfos){
						hostnames[datanodeCount++] = datanodeInfo.getHostName();
					}	
					HadoopFileSplit hadoopFileSplit = new HadoopFileSplit(absolutePath,new Long(index * blockSize).longValue(),new Long(blockSize).longValue(),hostnames);
			    	hadoopFileSplits.add(hadoopFileSplit);
			    	index++;
			    	}
	    	}	
		}catch(Exception e){
			e.printStackTrace();
		}
		return hadoopFileSplits;
	}
}
