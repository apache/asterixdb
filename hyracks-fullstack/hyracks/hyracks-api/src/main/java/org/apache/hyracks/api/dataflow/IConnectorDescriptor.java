/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.api.dataflow;

import java.io.Serializable;
import java.util.BitSet;

import org.apache.hyracks.api.application.ICCServiceContext;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.IPartitionCollector;
import org.apache.hyracks.api.comm.IPartitionWriterFactory;
import org.apache.hyracks.api.constraints.IConstraintAcceptor;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.ActivityCluster;

import com.fasterxml.jackson.databind.JsonNode;

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
     * Factory method to create the send side writer that writes into this
     * connector.
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
     * Factory metod to create the receive side reader that reads data from this
     * connector.
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
     * @param ac
     *            - Activity Cluster
     */
    public void contributeSchedulingConstraints(IConstraintAcceptor constraintAcceptor, ActivityCluster ac,
            ICCServiceContext ccServiceCtx);

    /**
     * Indicate which consumer partitions may receive data from the given
     * producer partition.
     */
    public void indicateTargetPartitions(int nProducerPartitions, int nConsumerPartitions, int producerIndex,
            BitSet targetBitmap);

    /**
     * Indicate which producer partitions are required for the given receiver.
     */
    public void indicateSourcePartitions(int nProducerPartitions, int nConsumerPartitions, int consumerIndex,
            BitSet sourceBitmap);

    /**
     * Indicate whether the connector is an all-producers-to-all-consumers connector
     */
    public boolean allProducersToAllConsumers();

    /**
     * Gets the display name.
     */
    public String getDisplayName();

    /**
     * Sets the display name.
     */
    public void setDisplayName(String displayName);

    /**
     * Translate this connector descriptor to JSON.
     *
     * @return
     */
    public JsonNode toJSON();

    /**
     * Sets the connector Id
     */
    public void setConnectorId(ConnectorDescriptorId cdId);
}
