/*
 * Copyright 2009-2013 by The Regents of the University of California
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
package edu.uci.ics.asterix.external.indexing.dataflow;

import edu.uci.ics.asterix.external.indexing.input.AbstractHDFSReader;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;

public class RCFileIndexingTupleParser extends AbstractIndexingTupleParser{

    private Integer rowNumber = -1;
    private Integer lastFileNumber = -1;
    private long lastByteLocation = -1;

    public RCFileIndexingTupleParser(IHyracksTaskContext ctx, ARecordType recType, IAsterixHDFSRecordParser deserializer)
            throws HyracksDataException {
        super(ctx, recType, deserializer);
        tb = new ArrayTupleBuilder(4);
        dos = tb.getDataOutput();
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void appendIndexingData(ArrayTupleBuilder tb,
            AbstractHDFSReader inReader) throws Exception {
        aMutableInt.setValue(inReader.getFileNumber());
        aMutableLong.setValue(inReader.getReaderPosition());
        //add file number
        tb.addField(intSerde, aMutableInt);
        //add record offset
        tb.addField(longSerde, aMutableLong);
        //add row number
        if(aMutableInt.getIntegerValue().equals(lastFileNumber) && aMutableLong.getLongValue() == lastByteLocation){
            rowNumber++;
        }else{
            lastFileNumber = aMutableInt.getIntegerValue();
            lastByteLocation = aMutableLong.getLongValue();
            rowNumber = 0;
        }
        aMutableInt.setValue(rowNumber);
        tb.addField(intSerde, aMutableInt);
    }

}
