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
package org.apache.asterix.column.common.row;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.FixedLengthTypeTrait;
import org.apache.hyracks.data.std.primitive.VarLengthTypeTrait;
import org.apache.hyracks.storage.am.lsm.btree.tuples.LSMBTreeTupleReference;

public class DummyLSMBTreeTupleReference extends LSMBTreeTupleReference {
    private static final ITypeTraits[] TYPE_TRAITS;
    private IValueReference data;

    static {
        TYPE_TRAITS = new ITypeTraits[2];
        TYPE_TRAITS[0] = new FixedLengthTypeTrait(0);
        TYPE_TRAITS[1] = VarLengthTypeTrait.INSTANCE;
    }

    public DummyLSMBTreeTupleReference() {
        super(TYPE_TRAITS, 1, false, null);
    }

    public void set(IValueReference data) {
        this.data = data;
    }

    @Override
    public void setFieldCount(int fieldCount) {
        //NoOp
    }

    @Override
    public int getFieldCount() {
        return 2;
    }

    @Override
    public byte[] getFieldData(int fIdx) {
        return data.getByteArray();
    }

    @Override
    public int getFieldStart(int fIdx) {
        return data.getStartOffset();
    }

    @Override
    public int getFieldLength(int fIdx) {
        return data.getLength();
    }

    @Override
    public boolean isAntimatter() {
        return false;
    }
}
