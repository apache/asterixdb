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
package org.apache.asterix.test.common;

import org.apache.hyracks.data.std.util.GrowableArray;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

public class TestTupleReference implements ITupleReference {
    private GrowableArray[] fields;
    private int[] offsets;

    public TestTupleReference(GrowableArray[] fields) {
        this.fields = fields;
        offsets = new int[fields.length];
    }

    public TestTupleReference(int numfields) {
        this.fields = new GrowableArray[numfields];
        for (int i = 0; i < numfields; i++) {
            fields[i] = new GrowableArray();
        }
        offsets = new int[fields.length];
    }

    public GrowableArray[] getFields() {
        return fields;
    }

    public void setFields(GrowableArray[] fields) {
        this.fields = fields;
    }

    @Override
    public int getFieldCount() {
        return fields.length;
    }

    @Override
    public byte[] getFieldData(int fIdx) {
        return fields[fIdx].getByteArray();
    }

    @Override
    public int getFieldStart(int fIdx) {
        return offsets[fIdx];
    }

    @Override
    public int getFieldLength(int fIdx) {
        return fields[fIdx].getLength();
    }

    public void reset() {
        for (GrowableArray field : fields) {
            field.reset();
        }
    }
}