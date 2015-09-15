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
package org.apache.asterix.external.indexing.dataflow;

import org.apache.asterix.external.indexing.input.AbstractHDFSReader;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;

public class TextOrSeqIndexingTupleParser extends AbstractIndexingTupleParser{
    public TextOrSeqIndexingTupleParser(IHyracksTaskContext ctx,
            ARecordType recType, IAsterixHDFSRecordParser deserializer)
            throws HyracksDataException {
        super(ctx, recType, deserializer);
        tb = new ArrayTupleBuilder(3);
        dos = tb.getDataOutput();
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void appendIndexingData(ArrayTupleBuilder tb,
            AbstractHDFSReader inReader) throws Exception {
        aMutableInt.setValue(inReader.getFileNumber());
        aMutableLong.setValue(inReader.getReaderPosition());

        tb.addField(intSerde, aMutableInt);
        tb.addField(longSerde, aMutableLong);
    }

}
