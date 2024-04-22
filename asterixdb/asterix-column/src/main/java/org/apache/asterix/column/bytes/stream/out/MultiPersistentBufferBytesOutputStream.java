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
package org.apache.asterix.column.bytes.stream.out;

import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnWriteMultiPageOp;

public final class MultiPersistentBufferBytesOutputStream extends AbstractMultiBufferBytesOutputStream {
    public MultiPersistentBufferBytesOutputStream(Mutable<IColumnWriteMultiPageOp> multiPageOpRef) {
        super(multiPageOpRef);
    }

    @Override
    protected ByteBuffer confiscateNewBuffer() throws HyracksDataException {
        return multiPageOpRef.getValue().confiscatePersistent();
    }

    @Override
    protected void preReset() throws HyracksDataException {
        if (allocatedBytes > 0) {
            /*
             * Ensure limit is set to current position and position is set to 0. This to ensure no unrelated bytes are
             * persisted to disk. Unrelated bytes are bytes that were in the ByteBuffer (when the column writer
             * confiscated it from the buffer cache) but those byte do not belong to the column that to be written.
             */
            currentBuf.flip();
            //Persist all buffers before resetting the stream
            multiPageOpRef.getValue().persist();
            allocatedBytes = 0;
            buffers.clear();
        }
    }

    @Override
    public void writeTo(OutputStream outputStream) {
        throw new IllegalAccessError("Persistent stream cannot be written to other stream");
    }
}
