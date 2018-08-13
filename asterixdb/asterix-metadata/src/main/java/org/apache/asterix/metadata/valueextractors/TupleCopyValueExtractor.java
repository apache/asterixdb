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

package org.apache.asterix.metadata.valueextractors;

import java.nio.ByteBuffer;

import org.apache.asterix.common.transactions.TxnId;
import org.apache.asterix.metadata.api.IValueExtractor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.tuples.TypeAwareTupleReference;
import org.apache.hyracks.storage.am.common.tuples.TypeAwareTupleWriter;

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
        this.tupleReference = tupleWriter.createTupleReference();
    }

    @Override
    public ITupleReference getValue(TxnId txnId, ITupleReference tuple)
            throws AlgebricksException, HyracksDataException {
        int numBytes = tupleWriter.bytesRequired(tuple);
        tupleBytes = new byte[numBytes];
        tupleWriter.writeTuple(tuple, tupleBytes, 0);
        buf = ByteBuffer.wrap(tupleBytes);
        tupleReference.resetByTupleOffset(buf.array(), 0);
        return tupleReference;
    }
}
