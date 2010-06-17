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

import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Counters.Counter;

import edu.uci.ics.hyracks.api.dataflow.IOpenableDataWriter;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePullable;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorEnvironment;
import edu.uci.ics.hyracks.api.job.JobPlan;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.context.HyracksContext;
import edu.uci.ics.hyracks.coreops.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.coreops.base.IOpenableDataWriterOperator;
import edu.uci.ics.hyracks.coreops.file.IRecordReader;
import edu.uci.ics.hyracks.coreops.util.DeserializedOperatorNodePushable;
import edu.uci.ics.hyracks.hadoop.util.HadoopFileSplit;

public abstract class AbstractHadoopFileScanOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;
    protected HadoopFileSplit[] splits;

    public AbstractHadoopFileScanOperatorDescriptor(JobSpecification spec, HadoopFileSplit[] splits,
            RecordDescriptor recordDescriptor) {
        super(spec, 0, 1);
        recordDescriptors[0] = recordDescriptor;
        this.splits = splits;
    }

    protected abstract IRecordReader createRecordReader(HadoopFileSplit fileSplit, RecordDescriptor desc)
            throws Exception;

    protected class FileScanOperator implements IOpenableDataWriterOperator {
        private IOpenableDataWriter<Object[]> writer;
        private int index;

        FileScanOperator(int index) {
            this.index = index;
        }

        @Override
        public void setDataWriter(int index, IOpenableDataWriter<Object[]> writer) {
            if (index != 0) {
                throw new IndexOutOfBoundsException("Invalid index: " + index);
            }
            this.writer = writer;
        }

        @Override
        public void open() throws HyracksDataException {
            HadoopFileSplit split = splits[index];
            RecordDescriptor desc = recordDescriptors[0];
            try {
                IRecordReader reader = createRecordReader(split, desc);
                if (desc == null) {
                    desc = recordDescriptors[0];
                }
                writer.open();
                try {
                    while (true) {
                        Object[] record = new Object[desc.getFields().length];
                        if (!reader.read(record)) {
                            break;
                        }
                        writer.writeData(record);
                    }
                } finally {
                    reader.close();
                    writer.close();
                }
            } catch (Exception e) {
                throw new HyracksDataException(e);
            }

        }

        @Override
        public void close() throws HyracksDataException {
            // do nothing
        }

        @Override
        public void writeData(Object[] data) throws HyracksDataException {
            throw new UnsupportedOperationException();
        }
    }

    protected Reporter createReporter() {
        return new Reporter() {
            @Override
            public Counter getCounter(Enum<?> name) {
                return null;
            }

            @Override
            public Counter getCounter(String group, String name) {
                return null;
            }

            @Override
            public InputSplit getInputSplit() throws UnsupportedOperationException {
                return null;
            }

            @Override
            public void incrCounter(Enum<?> key, long amount) {

            }

            @Override
            public void incrCounter(String group, String counter, long amount) {

            }

            @Override
            public void progress() {

            }

            @Override
            public void setStatus(String status) {

            }
        };
    }

    @Override
    public IOperatorNodePullable createPullRuntime(HyracksContext ctx, JobPlan plan, IOperatorEnvironment env,
            int partition) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IOperatorNodePushable createPushRuntime(HyracksContext ctx, JobPlan plan, IOperatorEnvironment env,
            int partition) {
        return new DeserializedOperatorNodePushable(ctx, new FileScanOperator(partition), plan, getActivityNodeId());
    }

    @Override
    public boolean supportsPullInterface() {
        return false;
    }

    @Override
    public boolean supportsPushInterface() {
        return true;
    }
}