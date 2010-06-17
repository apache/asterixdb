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
package edu.uci.ics.hyracks.hadoop.util;

import java.io.Serializable;

import org.apache.hadoop.fs.Path;

public class HadoopFileSplit implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String filePath;
    private long start;
	private long length;
    private String[] hosts;
    
    public HadoopFileSplit(String filePath, long start, long length, String[] hosts){
    	this.filePath = filePath;
    	this.start = start;
    	this.length = length;
    	this.hosts = hosts;
    }

	public String getFile() {
		return filePath;
	}

	public void setFile(String file) {
		this.filePath = file;
	}

	public long getStart() {
		return start;
	}

	public void setStart(long start) {
		this.start = start;
	}

	public long getLength() {
		return length;
	}

	public void setLength(long length) {
		this.length = length;
	}

	public String[] getHosts() {
		return hosts;
	}

	public void setHosts(String[] hosts) {
		this.hosts = hosts;
	}
	
	public String toString(){
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append(filePath + " " + start + " " + length +  "\n");
		for(String host : hosts){
			stringBuilder.append(host);
			stringBuilder.append(",");
		}
		return new String(stringBuilder);
	}
}
