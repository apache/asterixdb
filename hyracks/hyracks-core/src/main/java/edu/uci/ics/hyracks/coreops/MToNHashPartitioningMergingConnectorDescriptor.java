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
package edu.uci.ics.hyracks.coreops;

import edu.uci.ics.hyracks.api.comm.IConnectionDemultiplexer;
import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.dataflow.IEndpointDataWriterFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobPlan;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.comm.SortMergeFrameReader;
import edu.uci.ics.hyracks.context.HyracksContext;
import edu.uci.ics.hyracks.coreops.base.AbstractConnectorDescriptor;

public class MToNHashPartitioningMergingConnectorDescriptor extends AbstractConnectorDescriptor {
    private static final long serialVersionUID = 1L;

    private final ITuplePartitionComputerFactory tpcf;
    private final int[] sortFields;
    private final IBinaryComparatorFactory[] comparatorFactories;

    public MToNHashPartitioningMergingConnectorDescriptor(JobSpecification spec, ITuplePartitionComputerFactory tpcf,
            int[] sortFields, IBinaryComparatorFactory[] comparatorFactories) {
        super(spec);
        this.tpcf = tpcf;
        this.sortFields = sortFields;
        this.comparatorFactories = comparatorFactories;
    }

    @Override
    public IFrameWriter createSendSideWriter(HyracksContext ctx, JobPlan plan, IEndpointDataWriterFactory edwFactory,
            int index) throws HyracksDataException {
        JobSpecification spec = plan.getJobSpecification();
        final int consumerPartitionCount = spec.getConsumer(this).getPartitions().length;
        final HashDataWriter hashWriter = new HashDataWriter(ctx, consumerPartitionCount, edwFactory, spec
                .getConnectorRecordDescriptor(this), tpcf.createPartitioner());
        return hashWriter;
    }

    @Override
    public IFrameReader createReceiveSideReader(HyracksContext ctx, JobPlan plan, IConnectionDemultiplexer demux, int index)
            throws HyracksDataException {
        IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
        for(int i = 0; i < comparatorFactories.length; ++i) {
            comparators[i] = comparatorFactories[i].createBinaryComparator();
        }
        JobSpecification spec = plan.getJobSpecification();
        return new SortMergeFrameReader(ctx, demux, sortFields, comparators, spec.getConnectorRecordDescriptor(this));
    }
}