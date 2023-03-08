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
package org.apache.asterix.test.om.lazy;

import java.io.DataInputStream;
import java.io.IOException;

import org.apache.asterix.om.lazy.AbstractListLazyVisitablePointable;
import org.apache.asterix.om.lazy.FlatLazyVisitablePointable;
import org.apache.asterix.om.lazy.ILazyVisitablePointableVisitor;
import org.apache.asterix.om.lazy.RecordLazyVisitablePointable;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleInputStream;
import org.apache.hyracks.util.string.UTF8StringReader;
import org.apache.hyracks.util.string.UTF8StringUtil;

/**
 * Infers the type of single record using lazy visitable pointable
 */
public class RecordTypeInference implements ILazyVisitablePointableVisitor<IAType, String> {
    private final ByteArrayAccessibleInputStream in;
    private final DataInputStream dataIn;
    private final UTF8StringReader utf8Reader;

    public RecordTypeInference() {
        in = new ByteArrayAccessibleInputStream(new byte[] {}, 0, 0);
        dataIn = new DataInputStream(in);
        utf8Reader = new UTF8StringReader();
    }

    @Override
    public IAType visit(RecordLazyVisitablePointable pointable, String arg) throws HyracksDataException {
        String[] fieldNames = new String[pointable.getNumberOfChildren()];
        IAType[] fieldTypes = new IAType[pointable.getNumberOfChildren()];
        for (int i = 0; i < pointable.getNumberOfChildren(); i++) {
            pointable.nextChild();
            fieldNames[i] = deserializeString(pointable.getFieldName());
            fieldTypes[i] = pointable.getChildVisitablePointable().accept(this, fieldNames[i]);
        }
        // isOpen has to be false here to ensure that every field go to the closed part
        return new ARecordType(arg, fieldNames, fieldTypes, false);
    }

    @Override
    public IAType visit(AbstractListLazyVisitablePointable pointable, String arg) throws HyracksDataException {
        IAType itemType = BuiltinType.ANY;
        String itemTypeName = arg + "Item";
        for (int i = 0; i < pointable.getNumberOfChildren(); i++) {
            pointable.nextChild();
            IAType ithItemType = pointable.getChildVisitablePointable().accept(this, itemTypeName);
            if (itemType.getTypeTag() != ATypeTag.ANY && itemType.getTypeTag() != ithItemType.getTypeTag()) {
                throw new UnsupportedOperationException("Union types are not supported");
            }
            itemType = ithItemType;
        }
        return pointable.getTypeTag() == ATypeTag.ARRAY ? new AOrderedListType(itemType, arg)
                : new AUnorderedListType(itemType, arg);
    }

    @Override
    public IAType visit(FlatLazyVisitablePointable pointable, String arg) throws HyracksDataException {
        return BuiltinType.getBuiltinType(pointable.getTypeTag());
    }

    private String deserializeString(IValueReference stringValue) throws HyracksDataException {
        in.setContent(stringValue.getByteArray(), stringValue.getStartOffset(), stringValue.getLength());
        try {
            return UTF8StringUtil.readUTF8(dataIn, utf8Reader);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }
}