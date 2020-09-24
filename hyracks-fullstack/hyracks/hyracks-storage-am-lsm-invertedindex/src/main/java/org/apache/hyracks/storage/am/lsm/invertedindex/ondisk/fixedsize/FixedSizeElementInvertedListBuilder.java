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

package org.apache.hyracks.storage.am.lsm.invertedindex.ondisk.fixedsize;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.invertedindex.ondisk.AbstractInvertedListBuilder;
import org.apache.hyracks.storage.am.lsm.invertedindex.util.InvertedIndexUtils;

public class FixedSizeElementInvertedListBuilder extends AbstractInvertedListBuilder {
    private final int listElementSize;

    public FixedSizeElementInvertedListBuilder(ITypeTraits[] invListFields) throws HyracksDataException {
        super(invListFields);
        InvertedIndexUtils.verifyAllFixedSizeTypeTrait(invListFields);

        int tmp = 0;
        for (int i = 0; i < invListFields.length; i++) {
            tmp += invListFields[i].getFixedLength();
        }
        listElementSize = tmp;
    }

    @Override
    public boolean startNewList(ITupleReference tuple, int numTokenFields) {
        if (pos + listElementSize > targetBuf.length) {
            return false;
        } else {
            listSize = 0;
            return true;
        }
    }

    @Override
    public boolean appendElement(ITupleReference tuple, int numTokenFields, int numElementFields) {
        if (pos + listElementSize > targetBuf.length) {
            return false;
        }

        for (int i = 0; i < numElementFields; i++) {
            int field = numTokenFields + i;
            System.arraycopy(tuple.getFieldData(field), tuple.getFieldStart(field), targetBuf, pos,
                    tuple.getFieldLength(field));
        }

        listSize++;
        pos += listElementSize;

        return true;
    }

    @Override
    public boolean isFixedSize() {
        return true;
    }
}
