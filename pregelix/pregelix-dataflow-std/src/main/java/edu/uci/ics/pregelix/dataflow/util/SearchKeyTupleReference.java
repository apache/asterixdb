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
package edu.uci.ics.pregelix.dataflow.util;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

public class SearchKeyTupleReference implements ITupleReference {

    private byte[] copiedData;
    private int length;

    public void reset(byte[] data, int start, int len) {
        if (copiedData == null) {
            copiedData = new byte[len];
        }
        if (copiedData.length < len) {
            copiedData = new byte[len];
        }
        System.arraycopy(data, start, copiedData, 0, len);
        length = len;
    }

    @Override
    public int getFieldCount() {
        return 1;
    }

    @Override
    public byte[] getFieldData(int fIdx) {
        return copiedData;
    }

    @Override
    public int getFieldStart(int fIdx) {
        return 0;
    }

    @Override
    public int getFieldLength(int fIdx) {
        return length;
    }

}
