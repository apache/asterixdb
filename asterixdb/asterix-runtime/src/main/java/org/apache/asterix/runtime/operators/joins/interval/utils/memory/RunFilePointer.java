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
package org.apache.asterix.runtime.operators.joins.interval.utils.memory;

import java.util.Comparator;

import org.apache.hyracks.dataflow.std.structures.IResetable;

public final class RunFilePointer implements IResetable<RunFilePointer> {
    public static final int INVALID_ID = -1;
    private long fileOffset;
    private int tupleIndex;

    public static final Comparator<RunFilePointer> ASC = (tp1, tp2) -> {
        int c = (int) (tp1.getFileOffset() - tp2.getFileOffset());
        if (c == 0) {
            c = tp1.getTupleIndex() - tp2.getTupleIndex();
        }
        return c;
    };

    public static final Comparator<RunFilePointer> DESC = (tp1, tp2) -> {
        int c = (int) (tp2.getFileOffset() - tp1.getFileOffset());
        if (c == 0) {
            c = tp2.getTupleIndex() - tp1.getTupleIndex();
        }
        return c;
    };

    public RunFilePointer() {
        this(INVALID_ID, INVALID_ID);
    }

    public RunFilePointer(long fileOffset, int tupleId) {
        reset(fileOffset, tupleId);
    }

    public long getFileOffset() {
        return fileOffset;
    }

    public int getTupleIndex() {
        return tupleIndex;
    }

    @Override
    public void reset(RunFilePointer other) {
        reset(other.fileOffset, other.tupleIndex);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o == null || getClass() != o.getClass()) {
            return false;
        } else {
            final RunFilePointer that = (RunFilePointer) o;
            return fileOffset == that.fileOffset && tupleIndex == that.tupleIndex;
        }
    }

    @Override
    public int hashCode() {
        int result = (int) fileOffset;
        result = 31 * result + tupleIndex;
        return result;
    }

    public void reset(long fileOffset, int tupleId) {
        this.fileOffset = fileOffset;
        this.tupleIndex = tupleId;
    }

    @Override
    public String toString() {
        return "RunFilePointer(" + fileOffset + ", " + tupleIndex + ")";
    }

}
