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
package edu.uci.ics.hyracks.coreops.file;

import java.io.File;

import edu.uci.ics.hyracks.api.dataflow.IOpenableDataWriter;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePullable;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorEnvironment;
import edu.uci.ics.hyracks.api.job.JobPlan;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.context.HyracksContext;
import edu.uci.ics.hyracks.coreops.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.coreops.base.IOpenableDataWriterOperator;
import edu.uci.ics.hyracks.coreops.util.DeserializedOperatorNodePushable;

public abstract class AbstractFileWriteOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    protected class FileWriteOperator implements IOpenableDataWriterOperator {
        private int index;
        private IRecordWriter writer;

        FileWriteOperator(int index) {
            this.index = index;
        }

        @Override
        public void setDataWriter(int index, IOpenableDataWriter<Object[]> writer) {
            throw new IllegalArgumentException();
        }

        @Override
        public void open() throws HyracksDataException {
            FileSplit split = splits[index];
            try {
                writer = createRecordWriter(split.getLocalFile(), index);
            } catch (Exception e) {
                throw new HyracksDataException(e);
            }
        }

        @Override
        public void close() throws HyracksDataException {
            writer.close();
        }

        @Override
        public void writeData(Object[] data) throws HyracksDataException {
            try {
                writer.write(data);

            } catch (Exception e) {
                throw new HyracksDataException(e);
            }
        }
    }

    private static final long serialVersionUID = 1L;

    protected FileSplit[] splits;

    public FileSplit[] getSplits() {
		return splits;
	}

	public void setSplits(FileSplit[] splits) {
		this.splits = splits;
	}

	public AbstractFileWriteOperatorDescriptor(JobSpecification spec, FileSplit[] splits) {
        super(spec, 1, 0);
        this.splits = splits;
    }

    protected abstract IRecordWriter createRecordWriter(File file, int index) throws Exception;

    @Override
    public IOperatorNodePullable createPullRuntime(HyracksContext ctx, JobPlan plan, IOperatorEnvironment env,
            int partition) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IOperatorNodePushable createPushRuntime(HyracksContext ctx, JobPlan plan, IOperatorEnvironment env,
            int partition) {
        return new DeserializedOperatorNodePushable(ctx, new FileWriteOperator(partition), plan, getActivityNodeId());
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