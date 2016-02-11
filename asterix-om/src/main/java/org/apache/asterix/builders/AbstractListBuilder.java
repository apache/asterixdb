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
package org.apache.asterix.builders;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.asterix.dataflow.data.nontagged.serde.SerializerDeserializerUtil;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.util.NonTaggedFormatUtil;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.GrowableArray;
import org.apache.hyracks.storage.am.common.ophelpers.IntArrayList;

public abstract class AbstractListBuilder implements IAsterixListBuilder {
    protected final GrowableArray outputStorage;
    protected final DataOutputStream outputStream;
    protected final IntArrayList offsets;
    protected int metadataInfoSize;
    protected byte[] offsetArray;
    protected int offsetPosition;
    protected int headerSize;
    protected ATypeTag itemTypeTag;
    protected final ATypeTag listType;

    protected boolean fixedSize = false;
    protected int numberOfItems;

    public AbstractListBuilder(ATypeTag listType) {
        this.outputStorage = new GrowableArray();
        this.outputStream = (DataOutputStream) outputStorage.getDataOutput();
        this.offsets = new IntArrayList(10, 10);
        this.metadataInfoSize = 0;
        this.offsetArray = null;
        this.offsetPosition = 0;
        this.listType = listType;
    }

    @Override
    public void reset(AbstractCollectionType listType) {
        this.outputStorage.reset();
        this.offsetArray = null;
        this.offsets.clear();
        this.offsetPosition = 0;
        this.numberOfItems = 0;
        if (listType == null || listType.getItemType() == null) {
            this.itemTypeTag = ATypeTag.ANY;
            fixedSize = false;
        } else {
            this.itemTypeTag = listType.getItemType().getTypeTag();
            fixedSize = NonTaggedFormatUtil.isFixedSizedCollection(listType.getItemType());
        }
        headerSize = 2;
        metadataInfoSize = 8;
        // 8 = 4 (# of items) + 4 (the size of the list)
    }

    @Override
    public void addItem(IValueReference item) throws HyracksDataException {
        try {
            if (!fixedSize && (item.getByteArray()[0] != ATypeTag.SERIALIZED_NULL_TYPE_TAG || itemTypeTag == ATypeTag.ANY))
                this.offsets.add(outputStorage.getLength());
            if (itemTypeTag == ATypeTag.ANY
                    || (itemTypeTag == ATypeTag.NULL && item.getByteArray()[0] == ATypeTag.SERIALIZED_NULL_TYPE_TAG)) {
                this.numberOfItems++;
                this.outputStream.write(item.getByteArray(), item.getStartOffset(), item.getLength());
            } else if (item.getByteArray()[0] != ATypeTag.SERIALIZED_NULL_TYPE_TAG) {
                this.numberOfItems++;
                this.outputStream.write(item.getByteArray(), item.getStartOffset() + 1, item.getLength() - 1);
            }
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void write(DataOutput out, boolean writeTypeTag) throws HyracksDataException {
        try {
            if (!fixedSize)
                metadataInfoSize += offsets.size() * 4;
            if (offsetArray == null || offsetArray.length < metadataInfoSize)
                offsetArray = new byte[metadataInfoSize];

            SerializerDeserializerUtil.writeIntToByteArray(offsetArray,
                    headerSize + metadataInfoSize + outputStorage.getLength(), offsetPosition);
            SerializerDeserializerUtil.writeIntToByteArray(offsetArray, this.numberOfItems, offsetPosition + 4);

            if (!fixedSize) {
                offsetPosition += 8;
                for (int i = 0; i < offsets.size(); i++) {
                    SerializerDeserializerUtil.writeIntToByteArray(offsetArray,
                            offsets.get(i) + metadataInfoSize + headerSize, offsetPosition);
                    offsetPosition += 4;
                }
            }
            if (writeTypeTag) {
                out.writeByte(listType.serialize());
            }
            out.writeByte(itemTypeTag.serialize());
            out.write(offsetArray, 0, metadataInfoSize);
            out.write(outputStorage.getByteArray(), 0, outputStorage.getLength());
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }
}
