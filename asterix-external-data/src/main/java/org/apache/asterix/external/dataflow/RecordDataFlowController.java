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
import org.apache.asterix.external.api.IRecordFlowController;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;

public class RecordDataFlowController<T> extends AbstractDataFlowController implements IRecordFlowController<T> {

    protected IRecordDataParser<T> dataParser;
    protected IRecordReader<? extends T> recordReader;
    protected int numOfTupleFields = 1;

    @Override
    public void start(IFrameWriter writer) throws HyracksDataException {
        try {
            ArrayTupleBuilder tb = new ArrayTupleBuilder(numOfTupleFields);
            initializeTupleForwarder(writer);
            while (recordReader.hasNext()) {
                IRawRecord<? extends T> record = recordReader.next();
                tb.reset();
                dataParser.parse(record, tb.getDataOutput());
                tb.addFieldEndOffset();
                appendOtherTupleFields(tb);
                tupleForwarder.addTuple(tb);
            }
            tupleForwarder.close();
            recordReader.close();
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    protected void appendOtherTupleFields(ArrayTupleBuilder tb) throws Exception {
    }

    @Override
    public boolean stop() {
        return false;
    }

    @Override
    public boolean handleException(Throwable th) {
        return false;
    }

    @Override
    public void setRecordParser(IRecordDataParser<T> dataParser) {
        this.dataParser = dataParser;
    }

    @Override
    public void setRecordReader(IRecordReader<T> recordReader) throws Exception {
        this.recordReader = recordReader;
    }
}
