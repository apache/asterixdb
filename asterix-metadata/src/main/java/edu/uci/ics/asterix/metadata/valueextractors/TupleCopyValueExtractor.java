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

package edu.uci.ics.asterix.metadata.valueextractors;

import java.io.IOException;
import java.nio.ByteBuffer;

import edu.uci.ics.asterix.common.transactions.JobId;
import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.metadata.api.IValueExtractor;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.tuples.TypeAwareTupleReference;
import edu.uci.ics.hyracks.storage.am.common.tuples.TypeAwareTupleWriter;

/**
 * Returns a copy of the given input tuple.
 */
public class TupleCopyValueExtractor implements IValueExtractor<ITupleReference> {
    private final TypeAwareTupleWriter tupleWriter;
    private final TypeAwareTupleReference tupleReference;
    // TODO: Try avoid reallocating the tupleBytes.
    // TODO: Get rid of the ByteBuffer by chancing the TypeAwareTupleReference
    // reset() interface.
    private byte[] tupleBytes;
    private ByteBuffer buf;

    public TupleCopyValueExtractor(ITypeTraits[] typeTraits) {
        this.tupleWriter = new TypeAwareTupleWriter(typeTraits);
        this.tupleReference = (TypeAwareTupleReference) tupleWriter.createTupleReference();
    }

    @Override
    public ITupleReference getValue(JobId jobId, ITupleReference tuple) throws MetadataException, HyracksDataException,
            IOException {
        int numBytes = tupleWriter.bytesRequired(tuple);
        tupleBytes = new byte[numBytes];
        tupleWriter.writeTuple(tuple, tupleBytes, 0);
        buf = ByteBuffer.wrap(tupleBytes);
        tupleReference.resetByTupleOffset(buf, 0);
        return tupleReference;
    }
}
