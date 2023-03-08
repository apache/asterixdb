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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnWriteMultiPageOp;

public final class MultiTemporaryBufferBytesOutputStream extends AbstractMultiBufferBytesOutputStream {
    public MultiTemporaryBufferBytesOutputStream(Mutable<IColumnWriteMultiPageOp> multiPageOpRef) {
        super(multiPageOpRef);
    }

    @Override
    protected void preReset() {
        //NoOp
    }

    @Override
    protected ByteBuffer confiscateNewBuffer() throws HyracksDataException {
        return multiPageOpRef.getValue().confiscateTemporary();
    }

    @Override
    public void writeTo(OutputStream outputStream) throws IOException {
        int writtenSize = 0;
        for (int i = 0; i < currentBufferIndex + 1; i++) {
            ByteBuffer buffer = buffers.get(i);
            outputStream.write(buffer.array(), 0, buffer.position());
            writtenSize += buffer.position();
        }
        if (writtenSize != position) {
            //Sanity check
            throw new IllegalStateException("Size is different");
        }
    }
}
