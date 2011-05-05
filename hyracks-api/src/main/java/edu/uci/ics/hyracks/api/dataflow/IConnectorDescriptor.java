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
package edu.uci.ics.hyracks.api.dataflow;

import java.io.Serializable;
import java.util.BitSet;

import org.json.JSONException;
import org.json.JSONObject;

import edu.uci.ics.hyracks.api.application.ICCApplicationContext;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.comm.IPartitionCollector;
import edu.uci.ics.hyracks.api.comm.IPartitionWriterFactory;
import edu.uci.ics.hyracks.api.constraints.IConstraintAcceptor;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobActivityGraph;

/**
 * Connector that connects operators in a Job.
 * 
 * @author vinayakb
 */
public interface IConnectorDescriptor extends Serializable {
    /**
     * Gets the id of the connector.
     * 
     * @return
     */
    public ConnectorDescriptorId getConnectorId();

    /**
     * Factory method to create the send side writer that writes into this connector.
     * 
     * @param ctx
     *            Context
     * @param recordDesc
     *            Record Descriptor
     * @param edwFactory
     *            Endpoint writer factory.
     * @param index
     *            ordinal index of the data producer partition.
     * @param nProducerPartitions
     *            Number of partitions of the producing operator.
     * @param nConsumerPartitions
     *            Number of partitions of the consuming operator.
     * @return data writer.
     * @throws Exception
     */
    public IFrameWriter createPartitioner(IHyracksTaskContext ctx, RecordDescriptor recordDesc,
            IPartitionWriterFactory edwFactory, int index, int nProducerPartitions, int nConsumerPartitions)
            throws HyracksDataException;

    /**
     * Factory metod to create the receive side reader that reads data from this connector.
     * 
     * @param ctx
     *            Context
     * @param recordDesc
     *            Job plan
     * @param receiverIndex
     *            ordinal index of the data consumer partition
     * @param nProducerPartitions
     *            Number of partitions of the producing operator.
     * @param nConsumerPartitions
     *            Number of partitions of the consuming operator.
     * @return partition collector
     * @throws HyracksDataException
     */
    public IPartitionCollector createPartitionCollector(IHyracksTaskContext ctx, RecordDescriptor recordDesc,
            int receiverIndex, int nProducerPartitions, int nConsumerPartitions) throws HyracksDataException;

    /**
     * Contribute any scheduling constraints imposed by this connector
     * 
     * @param constraintAcceptor
     *            - Constraint Acceptor
     * @param plan
     *            - Job Plan
     */
    public void contributeSchedulingConstraints(IConstraintAcceptor constraintAcceptor, JobActivityGraph plan,
            ICCApplicationContext appCtx);

    /**
     * Indicate which consumer partitions may receive data from the given producer partition.
     */
    public void indicateTargetPartitions(int nProducerPartitions, int nConsumerPartitions, int producerIndex,
            BitSet targetBitmap);

    /**
     * Translate this connector descriptor to JSON.
     * 
     * @return
     * @throws JSONException
     */
    public JSONObject toJSON() throws JSONException;
}