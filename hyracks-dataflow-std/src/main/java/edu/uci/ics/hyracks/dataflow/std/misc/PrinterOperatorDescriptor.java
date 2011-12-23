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
package edu.uci.ics.hyracks.dataflow.std.misc;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOpenableDataWriter;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorEnvironment;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.IOpenableDataWriterOperator;
import edu.uci.ics.hyracks.dataflow.std.util.DeserializedOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.util.StringSerializationUtils;

public class PrinterOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;
    PrintWriter pw; 
    public int counter = 0;
    
    public PrinterOperatorDescriptor(JobSpecification spec) {
        super(spec, 1, 0);
    }

    private class PrinterOperator implements IOpenableDataWriterOperator {
        @Override
        public void open() throws HyracksDataException {
        	try {
				pw = new PrintWriter( new File("/home/pouria/Desktop/printerOutput.txt") );
			} catch (FileNotFoundException e) {
				e.printStackTrace();
				throw (new HyracksDataException("Exception Happened in Printer Open() method"));
			}
        }

        @Override
        public void close() throws HyracksDataException {
        	pw.println("Output Size (From Printer):\t"+counter);
        	pw.close();
        }

        @Override
        public void fail() throws HyracksDataException {
        }

        @Override
        public void writeData(Object[] data) throws HyracksDataException {
        	/*
            for (int i = 0; i < data.length; ++i) {
                System.err.print(StringSerializationUtils.toString(data[i]));
                System.err.print(", ");
            }
            System.err.println();
            */
        	for (int i = 0; i < data.length; ++i) {
                pw.print(String.valueOf(data[i]));
                pw.print(", ");
            }
        	pw.println("");
        	
        	
        	counter++;
        	if(counter % 20000 == 0){
        		System.err.println(counter+" records sent to output");
        	}
        }

        @Override
        public void setDataWriter(int index, IOpenableDataWriter<Object[]> writer) {
            throw new IllegalArgumentException();
        }
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx, IOperatorEnvironment env,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
        return new DeserializedOperatorNodePushable(ctx, new PrinterOperator(),
                recordDescProvider.getInputRecordDescriptor(getOperatorId(), 0));
    }
}