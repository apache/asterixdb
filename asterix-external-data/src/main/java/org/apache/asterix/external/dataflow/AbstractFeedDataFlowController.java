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
package org.apache.asterix.external.dataflow;

import java.util.Map;

import org.apache.asterix.common.parse.ITupleForwarder;
import org.apache.asterix.external.api.IDataFlowController;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;

public abstract class AbstractFeedDataFlowController implements IDataFlowController {
    protected FeedTupleForwarder tupleForwarder;
    protected IHyracksTaskContext ctx;
    protected Map<String, String> configuration;
    protected static final int NUMBER_OF_TUPLE_FIELDS = 1;
    protected ArrayTupleBuilder tb = new ArrayTupleBuilder(NUMBER_OF_TUPLE_FIELDS);

    @Override
    public ITupleForwarder getTupleForwarder() {
        return tupleForwarder;
    }

    @Override
    public void setTupleForwarder(ITupleForwarder tupleForwarder) {
        this.tupleForwarder = (FeedTupleForwarder) tupleForwarder;
    }

    protected void initializeTupleForwarder(IFrameWriter writer) throws HyracksDataException {
        tupleForwarder.initialize(ctx, writer);
    }

    @Override
    public void configure(Map<String, String> configuration, IHyracksTaskContext ctx) {
        this.configuration = configuration;
        this.ctx = ctx;
    }

    @Override
    public boolean pause() {
        tupleForwarder.pause();
        return true;
    }

    @Override
    public boolean resume() {
        tupleForwarder.resume();
        return true;
    }

    public void flush() throws HyracksDataException {
        tupleForwarder.flush();
    }
}
