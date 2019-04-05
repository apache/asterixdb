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

import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.CleanupUtils;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;

public class RecordDataFlowController<T> extends AbstractDataFlowController {

    protected final IRecordDataParser<T> dataParser;
    protected final IRecordReader<? extends T> recordReader;
    protected final int numOfTupleFields;

    public RecordDataFlowController(IHyracksTaskContext ctx, IRecordDataParser<T> dataParser,
            IRecordReader<? extends T> recordReader, int numOfTupleFields) {
        super(ctx);
        this.dataParser = dataParser;
        this.recordReader = recordReader;
        this.numOfTupleFields = numOfTupleFields;
    }

    @Override
    public void start(IFrameWriter writer) throws HyracksDataException {
        try {
            ArrayTupleBuilder tb = new ArrayTupleBuilder(numOfTupleFields);
            TupleForwarder tupleForwarder = new TupleForwarder(ctx, writer);
            while (recordReader.hasNext()) {
                IRawRecord<? extends T> record = recordReader.next();
                tb.reset();
                dataParser.parse(record, tb.getDataOutput());
                tb.addFieldEndOffset();
                appendOtherTupleFields(tb);
                tupleForwarder.addTuple(tb);
            }
            tupleForwarder.complete();
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        } finally {
            CleanupUtils.close(recordReader, null);
        }
    }

    protected void appendOtherTupleFields(ArrayTupleBuilder tb) throws Exception {
    }
}
