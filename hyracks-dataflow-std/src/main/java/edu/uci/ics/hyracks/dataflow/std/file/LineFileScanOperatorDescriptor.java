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
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public class LineFileScanOperatorDescriptor extends
        AbstractFileScanOperatorDescriptor {
    private static class LineReaderImpl extends  RecordReader {
        private File file;

        LineReaderImpl(File file) throws Exception {
        	super(new Object[]{file});
        	this.file = file;
        }

		@Override
		public InputStream createInputStream(Object[] args) throws Exception{
			this.file = (File)args[0];
			return new FileInputStream(file) ;
		}
       }

    private static final long serialVersionUID = 1L;

    public LineFileScanOperatorDescriptor(JobSpecification spec,
            FileSplit[] splits, RecordDescriptor recordDescriptor) {
        super(spec, splits, recordDescriptor);
    }

    @Override
    protected IRecordReader createRecordReader(File file, RecordDescriptor desc)
            throws Exception {
        return new LineReaderImpl(file);
    }
    
    @Override
	protected void configure() throws Exception {
		// currently a no-op, but is meant to initialize , if required before it is asked 
		// to create a record reader
		// this is executed at the node and is useful for operators that could not be 
		// initialized from the client completely, because of lack of information specific 
		// to the node where the operator gets executed. 
		
	}
}