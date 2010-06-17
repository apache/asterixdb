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

import org.json.JSONException;
import org.json.JSONObject;

import edu.uci.ics.hyracks.api.comm.IConnectionDemultiplexer;
import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobPlan;
import edu.uci.ics.hyracks.context.HyracksContext;

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
     * @param plan
     *            Job plan
     * @param edwFactory
     *            Endpoint writer factory.
     * @param index
     *            ordinal index of the data producer partition.
     * @return data writer.
     * @throws Exception
     */
    public IFrameWriter createSendSideWriter(HyracksContext ctx, JobPlan plan, IEndpointDataWriterFactory edwFactory,
            int index) throws HyracksDataException;

    /**
     * Factory metod to create the receive side reader that reads data from this connector.
     * 
     * @param ctx
     *            Context
     * @param plan
     *            Job plan
     * @param demux
     *            Connection Demultiplexer
     * @param index
     *            ordinal index of the data consumer partition
     * @return data reader
     * @throws HyracksDataException
     */
    public IFrameReader createReceiveSideReader(HyracksContext ctx, JobPlan plan, IConnectionDemultiplexer demux,
            int index) throws HyracksDataException;

    /**
     * Translate this connector descriptor to JSON.
     * 
     * @return
     * @throws JSONException
     */
    public JSONObject toJSON() throws JSONException;
}