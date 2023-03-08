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
package org.apache.asterix.om.lazy;

import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.utils.NonTaggedFormatUtil;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.VoidPointable;

/**
 * This implementation is to handle {@link ATypeTag#OBJECT} with open fields only
 */
public class RecordLazyVisitablePointable extends AbstractLazyNestedVisitablePointable {
    protected final VoidPointable currentFieldName;
    protected final AbstractLazyVisitablePointable openVisitable;
    private int openValuesOffset;
    private int numberOfOpenChildren;

    public RecordLazyVisitablePointable(boolean tagged) {
        super(tagged, ATypeTag.OBJECT);
        currentFieldName = new VoidPointable();
        openVisitable = new GenericLazyVisitablePointable();
    }

    @Override
    public void nextChild() throws HyracksDataException {
        byte[] data = getByteArray();

        //set field name
        int fieldNameLength = NonTaggedFormatUtil.getFieldValueLength(data, openValuesOffset, ATypeTag.STRING, false);
        currentFieldName.set(data, openValuesOffset, fieldNameLength);
        openValuesOffset += fieldNameLength;

        //set Type tag
        currentChildTypeTag = data[openValuesOffset];

        //set value
        int valueLength = NonTaggedFormatUtil.getFieldValueLength(data, openValuesOffset, getChildTypeTag(), true) + 1;
        currentValue.set(data, openValuesOffset, valueLength);
        openValuesOffset += valueLength;
    }

    @Override
    public boolean isTaggedChild() {
        return true;
    }

    @Override
    public int getNumberOfChildren() {
        return numberOfOpenChildren;
    }

    public IValueReference getFieldName() {
        return currentFieldName;
    }

    @Override
    public AbstractLazyVisitablePointable getChildVisitablePointable() throws HyracksDataException {
        openVisitable.set(getChildValue());
        return openVisitable;
    }

    @Override
    public final <R, T> R accept(ILazyVisitablePointableVisitor<R, T> visitor, T arg) throws HyracksDataException {
        return visitor.visit(this, arg);
    }

    @Override
    void init(byte[] data, int offset, int length) {
        initOpenPart(data, offset);
    }

    /* ******************************************************
     * Init Open part
     * ******************************************************
     */
    protected int initOpenPart(byte[] data, int pointer) {
        //+1 for type tag and +4 for the length
        int skipTypeTag = isTagged() ? 1 : 0;
        int currentPointer = pointer + skipTypeTag + 4;

        boolean isExpanded = data[currentPointer] == 1;
        //Advance to numberOfClosedChildren or
        currentPointer++;

        if (isExpanded) {
            /*
             * -(isTagged() ? 0 : 1) because the open part is computed - in the record builder - with the
             * assumption that the record type tag is always there. Hence, if the record contains a type tag at the
             * beginning of the record, we subtract 0 (noop). However, if the record doesn't contain a tag, then we
             * need to subtract by 1 to get the correct offset of the open part (i.e., as if the tag existed).
             */
            int openPartStart =
                    pointer + AInt32SerializerDeserializer.getInt(data, currentPointer) - (isTagged() ? 0 : 1);
            //Skip open part offset to the beginning of closed part
            currentPointer += 4;
            //Number of children in the open part
            numberOfOpenChildren = AInt32SerializerDeserializer.getInt(data, openPartStart);
            //Skip the numberOfOpenChildren and the hashOffsetPair to the first open value
            openValuesOffset = openPartStart + 4 + 8 * numberOfOpenChildren;
        } else {
            numberOfOpenChildren = 0;
        }

        return currentPointer;
    }
}
