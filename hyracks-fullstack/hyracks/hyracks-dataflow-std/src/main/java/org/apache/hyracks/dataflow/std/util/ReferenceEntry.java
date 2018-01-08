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
package org.apache.hyracks.dataflow.std.util;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputer;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;

public class ReferenceEntry {
    private final int runid;
    private IFrameTupleAccessor acccessor;
    private int tupleIndex;
    private final int[] tPointers;
    private final int normalizedKeyLength;

    public ReferenceEntry(int runid, FrameTupleAccessor fta, int tupleIndex, int[] keyFields,
            INormalizedKeyComputer nmkComputer) {
        super();
        this.runid = runid;
        this.acccessor = fta;
        this.normalizedKeyLength =
                nmkComputer != null ? nmkComputer.getNormalizedKeyProperties().getNormalizedKeyLength() : 0;
        this.tPointers = new int[normalizedKeyLength + 2 * keyFields.length];
        if (fta != null) {
            initTPointer(fta, tupleIndex, keyFields, nmkComputer);
        }
    }

    public int getRunid() {
        return runid;
    }

    public IFrameTupleAccessor getAccessor() {
        return acccessor;
    }

    public void setAccessor(IFrameTupleAccessor fta) {
        this.acccessor = fta;
    }

    public int[] getTPointers() {
        return tPointers;
    }

    public int getTupleIndex() {
        return tupleIndex;
    }

    public void setTupleIndex(int tupleIndex, int[] keyFields, INormalizedKeyComputer nmkComputer) {
        initTPointer(acccessor, tupleIndex, keyFields, nmkComputer);
    }

    private void initTPointer(IFrameTupleAccessor fta, int tupleIndex, int[] keyFields,
            INormalizedKeyComputer nmkComputer) {
        this.tupleIndex = tupleIndex;
        byte[] b1 = fta.getBuffer().array();
        for (int f = 0; f < keyFields.length; ++f) {
            int fIdx = keyFields[f];
            tPointers[2 * f + normalizedKeyLength] = fta.getAbsoluteFieldStartOffset(tupleIndex, fIdx);
            tPointers[2 * f + normalizedKeyLength + 1] = fta.getFieldLength(tupleIndex, fIdx);
            if (f == 0 && nmkComputer != null) {
                nmkComputer.normalize(b1, tPointers[normalizedKeyLength], tPointers[normalizedKeyLength + 1], tPointers,
                        0);
            }
        }
    }
}
