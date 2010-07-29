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
package edu.uci.ics.hyracks.dataflow.std.file;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public  abstract class RecordReader implements IRecordReader {

    private final BufferedReader bufferedReader;
	private InputStream inputStream;
	
	@Override
	public void close() {
        try {
        	bufferedReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean read(Object[] record) throws Exception {
        String line = bufferedReader.readLine();
        if (line == null) {
            return false;
        }
        record[0] = line;
        return true;
    }
	
	public abstract InputStream createInputStream(Object[] args) throws Exception;
	
	public RecordReader(Object[] args) throws Exception{
		try{
			bufferedReader = new BufferedReader(new InputStreamReader(createInputStream(args)));
		}catch(Exception e){
			e.printStackTrace();
			throw e;
		}
	}
	
}
