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

package org.apache.hyracks.storage.am.rtree.tuples;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.common.tuples.TypeAwareTupleWriter;
import org.apache.hyracks.util.encoding.VarLenIntEncoderDecoder;

public class RTreeTypeAwareTupleWriter extends TypeAwareTupleWriter {

    public RTreeTypeAwareTupleWriter(ITypeTraits[] typeTraits) {
        super(typeTraits);
    }

    public int writeTupleFields(ITreeIndexTupleReference[] refs, int startField, ByteBuffer targetBuf, int targetOff) {
        int runner = targetOff;
        int nullFlagsBytes = getNullFlagsBytes(refs.length);
        // write null indicator bits
        for (int i = 0; i < nullFlagsBytes; i++) {
            targetBuf.put(runner++, (byte) 0);
        }

        // write field slots for variable length fields
        // since the r-tree has fixed length keys, we don't actually need this?
        for (int i = startField; i < startField + refs.length; i++) {
            if (!typeTraits[i].isFixedLength()) {
                runner += VarLenIntEncoderDecoder.encode(refs[i].getFieldLength(i), targetBuf.array(), runner);
            }
        }

        // write data
        for (int i = 0; i < refs.length; i++) {
            System.arraycopy(refs[i].getFieldData(i), refs[i].getFieldStart(i), targetBuf.array(), runner,
                    refs[i].getFieldLength(i));
            runner += refs[i].getFieldLength(i);
        }
        return runner - targetOff;

    }
}
