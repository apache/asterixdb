/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.common.transactions;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

public class PrimaryKeyTupleReference implements ITupleReference {
    private byte[] fieldData;
    private int start;
    private int length;

    public void reset(byte[] fieldData, int start, int length) {
        this.fieldData = fieldData;
        this.start = start;
        this.length = length;
    }

    @Override
    public int getFieldCount() {
        return 1;
    }

    @Override
    public byte[] getFieldData(int fIdx) {
        return fieldData;
    }

    @Override
    public int getFieldStart(int fIdx) {
        return start;
    }

    @Override
    public int getFieldLength(int fIdx) {
        return length;
    }

}
