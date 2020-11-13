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

import org.apache.asterix.external.api.IStreamDataParser;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.storage.am.common.api.ITupleFilter;
import org.apache.hyracks.storage.am.common.tuples.ReferenceFrameTupleReference;

public class StreamDataFlowController extends AbstractDataFlowController {
    private final IStreamDataParser dataParser;

    public StreamDataFlowController(IHyracksTaskContext ctx, IStreamDataParser dataParser) {
        super(ctx);
        this.dataParser = dataParser;
    }

    @Override
    public void start(IFrameWriter writer, ITupleFilter tupleFilter, long outputLimit) throws HyracksDataException {
        try {
            processedTuples = 0;
            ArrayTupleBuilder tb = new ArrayTupleBuilder(1);
            boolean tupleFilterExists = tupleFilter != null;
            ArrayTupleReference tupleRef = tupleFilterExists ? new ArrayTupleReference() : null;
            ReferenceFrameTupleReference frameTupleRef = tupleFilterExists ? new ReferenceFrameTupleReference() : null;
            TupleForwarder tupleForwarder = new TupleForwarder(ctx, writer);
            while (outputLimit < 0 || processedTuples < outputLimit) {
                tb.reset();
                if (!dataParser.parse(tb.getDataOutput())) {
                    break;
                }
                tb.addFieldEndOffset();

                if (tupleFilterExists) {
                    tupleRef.reset(tb.getFieldEndOffsets(), tb.getByteArray());
                    frameTupleRef.reset(tupleRef);
                    if (!tupleFilter.accept(frameTupleRef)) {
                        continue;
                    }
                }

                tupleForwarder.addTuple(tb);
                processedTuples++;
            }
            tupleForwarder.complete();
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }
}
