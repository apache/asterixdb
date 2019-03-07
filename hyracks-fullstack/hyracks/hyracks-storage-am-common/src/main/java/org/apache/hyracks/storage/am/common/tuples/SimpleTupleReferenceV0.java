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

package org.apache.hyracks.storage.am.common.tuples;

import org.apache.hyracks.data.std.primitive.ShortPointable;

/**
 * This class is only used to read tuple references from log version 0
 */
public class SimpleTupleReferenceV0 extends SimpleTupleReference {

    @Override
    public int getFieldLength(int fIdx) {
        if (fIdx == 0) {
            return ShortPointable.getShort(buf, tupleStartOff + nullFlagsBytes);
        } else {
            return ShortPointable.getShort(buf, tupleStartOff + nullFlagsBytes + fIdx * 2)
                    - ShortPointable.getShort(buf, tupleStartOff + nullFlagsBytes + ((fIdx - 1) * 2));
        }
    }

    @Override
    public int getFieldStart(int fIdx) {
        if (fIdx == 0) {
            return tupleStartOff + nullFlagsBytes + fieldSlotsBytes;
        } else {
            return tupleStartOff + nullFlagsBytes + fieldSlotsBytes
                    + ShortPointable.getShort(buf, tupleStartOff + nullFlagsBytes + ((fIdx - 1) * 2));
        }
    }

    @Override
    protected int getFieldSlotsBytes() {
        return fieldCount * 2;
    }

    @Override
    public int getTupleSize() {
        return nullFlagsBytes + fieldSlotsBytes
                + ShortPointable.getShort(buf, tupleStartOff + nullFlagsBytes + (fieldCount - 1) * 2);
    }
}
