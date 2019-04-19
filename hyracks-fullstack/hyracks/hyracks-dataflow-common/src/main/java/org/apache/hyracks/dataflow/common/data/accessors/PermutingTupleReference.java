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

package org.apache.hyracks.dataflow.common.data.accessors;

public class PermutingTupleReference implements ITupleReference {

    private final int[] fieldPermutation;
    private ITupleReference sourceTuple;

    public PermutingTupleReference(int[] fieldPermutation) {
        this.fieldPermutation = fieldPermutation;
    }

    public void reset(ITupleReference sourceTuple) {
        this.sourceTuple = sourceTuple;
    }

    @Override
    public int getFieldCount() {
        return fieldPermutation.length;
    }

    @Override
    public byte[] getFieldData(int fIdx) {
        return sourceTuple.getFieldData(fieldPermutation[fIdx]);
    }

    @Override
    public int getFieldStart(int fIdx) {
        return sourceTuple.getFieldStart(fieldPermutation[fIdx]);
    }

    @Override
    public int getFieldLength(int fIdx) {
        return sourceTuple.getFieldLength(fieldPermutation[fIdx]);
    }
}
