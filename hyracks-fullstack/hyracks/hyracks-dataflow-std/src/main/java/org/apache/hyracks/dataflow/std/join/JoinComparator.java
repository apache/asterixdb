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
package org.apache.hyracks.dataflow.std.join;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparator;
import org.apache.hyracks.api.exceptions.HyracksDataException;

class JoinComparator implements ITuplePairComparator {
    private final IBinaryComparator bComparator;
    private final int field0;
    private final int field1;

    public JoinComparator(IBinaryComparator bComparator, int field0, int field1) {
        this.bComparator = bComparator;
        this.field0 = field0;
        this.field1 = field1;
    }

    @Override
    public int compare(IFrameTupleAccessor accessor0, int tIndex0, IFrameTupleAccessor accessor1, int tIndex1)
            throws HyracksDataException {
        int tStart0 = accessor0.getTupleStartOffset(tIndex0);
        int fStartOffset0 = accessor0.getFieldSlotsLength() + tStart0;

        int tStart1 = accessor1.getTupleStartOffset(tIndex1);
        int fStartOffset1 = accessor1.getFieldSlotsLength() + tStart1;

        int fStart0 = accessor0.getFieldStartOffset(tIndex0, field0);
        int fEnd0 = accessor0.getFieldEndOffset(tIndex0, field0);
        int fLen0 = fEnd0 - fStart0;

        int fStart1 = accessor1.getFieldStartOffset(tIndex1, field1);
        int fEnd1 = accessor1.getFieldEndOffset(tIndex1, field1);
        int fLen1 = fEnd1 - fStart1;

        int c = bComparator.compare(accessor0.getBuffer().array(), fStart0 + fStartOffset0, fLen0,
                accessor1.getBuffer().array(), fStart1 + fStartOffset1, fLen1);
        if (c != 0) {
            return c;
        }
        return 0;
    }
}
