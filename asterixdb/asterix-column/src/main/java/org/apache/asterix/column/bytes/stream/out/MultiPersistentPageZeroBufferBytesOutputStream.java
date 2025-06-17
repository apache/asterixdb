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

public final class MultiPersistentPageZeroBufferBytesOutputStream extends AbstractMultiBufferBytesOutputStream {
    public MultiPersistentPageZeroBufferBytesOutputStream(Mutable<IColumnWriteMultiPageOp> multiPageOpRef) {
        super(multiPageOpRef);
    }

    @Override
    protected ByteBuffer confiscateNewBuffer() throws HyracksDataException {
        return multiPageOpRef.getValue().confiscatePageZeroPersistent();
    }

    public void reset(int requiredPageSegments) throws HyracksDataException {
        preReset();
        allocateBuffers(requiredPageSegments); // these many buffers are required for page zero segments
    }

    @Override
    protected void preReset() {
        if (allocatedBytes > 0 || !buffers.isEmpty()) {
            //This should not be the case, with the pageZero segments.
            //As the stream should be finished after the flush.
            throw new IllegalStateException("PageZero segments should already be finished after flush");
        }
    }

    @Override
    public void writeTo(OutputStream outputStream) {
        throw new IllegalAccessError("Persistent stream cannot be written to other stream");
    }
}
