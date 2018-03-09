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
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.external.api.IRecordWithPKDataParser;
import org.apache.asterix.external.util.FeedLogManager;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;

public class ChangeFeedDataFlowController<T> extends FeedRecordDataFlowController<T> {

    private final IRecordWithPKDataParser<T> dataParser;

    public ChangeFeedDataFlowController(final IHyracksTaskContext ctx, final FeedLogManager feedLogManager,
            final int numOfOutputFields, final IRecordWithPKDataParser<T> dataParser,
            final IRecordReader<T> recordReader) throws HyracksDataException {
        super(ctx, feedLogManager, numOfOutputFields, dataParser, recordReader);
        this.dataParser = dataParser;
    }

    @Override
    protected void addPrimaryKeys(final ArrayTupleBuilder tb, final IRawRecord<? extends T> record)
            throws HyracksDataException {
        dataParser.appendKeys(tb, record);
    }
}
