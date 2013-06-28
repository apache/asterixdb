/*
 * Copyright 2009-2013 by The Regents of the University of California
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

package edu.uci.ics.pregelix.dataflow;

import java.nio.ByteBuffer;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import edu.uci.ics.pregelix.dataflow.base.IConfigurationFactory;
import edu.uci.ics.pregelix.dataflow.util.IterationUtils;

public class TerminationStateWriterOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(TerminationStateWriterOperatorDescriptor.class.getName());

    private final IConfigurationFactory confFactory;
    private final String jobId;

    public TerminationStateWriterOperatorDescriptor(JobSpecification spec, IConfigurationFactory confFactory,
            String jobId) {
        super(spec, 1, 0);
        this.confFactory = confFactory;
        this.jobId = jobId;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        return new AbstractUnaryInputSinkOperatorNodePushable() {
            private Configuration conf = confFactory.createConfiguration(ctx);
            private boolean terminate = true;

            @Override
            public void open() throws HyracksDataException {
                terminate = true;
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                terminate = false;
            }

            @Override
            public void fail() throws HyracksDataException {

            }

            @Override
            public void close() throws HyracksDataException {
                IterationUtils.writeTerminationState(conf, jobId, terminate);
                LOGGER.info("close termination state");
                if (terminate)
                    LOGGER.info("write termination to HDFS");
            }

        };
    }
}
