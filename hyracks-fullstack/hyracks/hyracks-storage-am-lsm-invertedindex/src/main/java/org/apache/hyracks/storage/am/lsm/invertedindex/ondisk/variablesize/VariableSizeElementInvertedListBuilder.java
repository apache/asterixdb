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

package org.apache.hyracks.storage.am.lsm.invertedindex.ondisk.variablesize;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.storage.am.common.tuples.TypeAwareTupleWriter;
import org.apache.hyracks.storage.am.lsm.invertedindex.ondisk.AbstractInvertedListBuilder;
import org.apache.hyracks.storage.am.lsm.invertedindex.util.InvertedIndexUtils;

// The last 4 bytes in the frame is reserved for the end offset (exclusive) of the last record in the current frame
// i.e. the trailing space after the last record and before the last 4 bytes will be treated as empty
public class VariableSizeElementInvertedListBuilder extends AbstractInvertedListBuilder {
    private ITreeIndexTupleWriter writer;
    protected final ITypeTraits[] allFields;

    // The tokenTypeTraits is necessary because the underlying TypeAwareTupleWriter requires all the type traits of the tuple
    // even if the first a few fields in the tuple are never accessed by the writer
    public VariableSizeElementInvertedListBuilder(ITypeTraits[] tokenTypeTraits, ITypeTraits[] invListFields)
            throws HyracksDataException {
        super(invListFields);

        this.allFields = new ITypeTraits[invListFields.length + tokenTypeTraits.length];
        for (int i = 0; i < tokenTypeTraits.length; i++) {
            allFields[i] = tokenTypeTraits[i];
        }
        for (int i = 0; i < invListFields.length; i++) {
            allFields[i + tokenTypeTraits.length] = invListFields[i];
        }
        this.writer = new TypeAwareTupleWriter(allFields);

        InvertedIndexUtils.verifyHasVarSizeTypeTrait(invListFields);
    }

    @Override
    public boolean startNewList(ITupleReference tuple, int numTokenFields) {
        if (!checkEnoughSpace(tuple, numTokenFields, tuple.getFieldCount() - numTokenFields)) {
            return false;
        } else {
            listSize = 0;
            return true;
        }
    }

    private boolean checkEnoughSpace(ITupleReference tuple, int numTokenFields, int numElementFields) {
        int numBytesRequired = writer.bytesRequired(tuple, numTokenFields, numElementFields);
        return checkEnoughSpace(numBytesRequired);
    }

    private boolean checkEnoughSpace(int numBytesRequired) {
        // The last 4 bytes are reserved for the end offset of the last record in the current page
        if (pos + numBytesRequired + 4 > targetBuf.length) {
            return false;
        }

        return true;
    }

    @Override
    public boolean appendElement(ITupleReference tuple, int numTokenFields, int numElementFields) {
        int numBytesRequired = writer.bytesRequired(tuple, numTokenFields, numElementFields);

        if (!checkEnoughSpace(numBytesRequired)) {
            return false;
        }

        pos += writer.writeTupleFields(tuple, numTokenFields, numElementFields, targetBuf, pos);
        listSize++;

        InvertedIndexUtils.setInvertedListFrameEndOffset(targetBuf, pos);
        return true;
    }

    @Override
    public boolean isFixedSize() {
        return false;
    }
}
