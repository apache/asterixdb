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

package org.apache.asterix.om.pointables.cast;

import java.io.IOException;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.om.pointables.AFlatValueCastingPointable;
import org.apache.asterix.om.pointables.AListCastingPointable;
import org.apache.asterix.om.pointables.ARecordCastingPointable;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.pointables.visitor.ICastingPointableVisitor;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

/**
 * This class is a ICastingPointableVisitor implementation which recursively
 * visit a given record, list or flat value of a given type, and cast it to a
 * specified type. For example:
 * A record { "hobby": {{"music", "coding"}}, "id": "001", "name":
 * "Person Three"} which confirms to closed type ( id: string, name: string,
 * hobby: {{string}}? ) can be casted to a open type (id: string )
 * Since the open/closed part of a record has a completely different underlying
 * memory/storage layout, the visitor will change the layout as specified at
 * runtime.
 */
public class ACastingPointableVisitor implements ICastingPointableVisitor<Void, CastResult> {

    private final ArrayBackedValueStorage tempBuffer = new ArrayBackedValueStorage();
    private final ArrayBackedValueStorage castBuffer = new ArrayBackedValueStorage();
    private final boolean strictDemote;
    private final SourceLocation sourceLoc;

    public ACastingPointableVisitor(boolean strictDemote, SourceLocation sourceLoc) {
        this.strictDemote = strictDemote;
        this.sourceLoc = sourceLoc;
    }

    public static ACastingPointableVisitor strictCasting(SourceLocation sourceLoc) {
        return new ACastingPointableVisitor(true, sourceLoc);
    }

    public static ACastingPointableVisitor laxCasting(SourceLocation sourceLoc) {
        return new ACastingPointableVisitor(false, sourceLoc);
    }

    @Override
    public Void visit(AListCastingPointable list, CastResult castResult) throws HyracksDataException {
        AbstractCollectionType resultType;
        switch (castResult.getOutType().getTypeTag()) {
            case ANY:
                resultType = list.ordered() ? DefaultOpenFieldType.NESTED_OPEN_AORDERED_LIST_TYPE
                        : DefaultOpenFieldType.NESTED_OPEN_AUNORDERED_LIST_TYPE;
                break;
            case ARRAY:
            case MULTISET:
                resultType = (AbstractCollectionType) castResult.getOutType();
                break;
            default:
                throw new RuntimeDataException(ErrorCode.TYPE_CONVERT, sourceLoc,
                        list.ordered() ? ATypeTag.ARRAY : ATypeTag.MULTISET, castResult.getOutType().getTypeTag());
        }

        list.castList(castResult.getOutPointable(), resultType, this);
        return null;
    }

    @Override
    public Void visit(ARecordCastingPointable record, CastResult castResult) throws HyracksDataException {
        ARecordType resultType;
        IAType oType = castResult.getOutType();
        switch (oType.getTypeTag()) {
            case ANY:
                resultType = DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE;
                break;
            case OBJECT:
                resultType = (ARecordType) oType;
                break;
            default:
                throw new RuntimeDataException(ErrorCode.TYPE_CONVERT, sourceLoc, ATypeTag.OBJECT, oType.getTypeTag());
        }

        record.castRecord(castResult.getOutPointable(), resultType, this);
        return null;
    }

    @Override
    public Void visit(AFlatValueCastingPointable flatValue, CastResult castResult) throws HyracksDataException {
        IValueReference val;
        if (flatValue.isTagged()) {
            val = flatValue;
        } else {
            tempBuffer.reset();
            try {
                tempBuffer.getDataOutput().writeByte(flatValue.getTag().serialize());
                tempBuffer.getDataOutput().write(flatValue.getByteArray(), flatValue.getStartOffset(),
                        flatValue.getLength());
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            }
            val = tempBuffer;
        }
        IAType oType = castResult.getOutType();
        IPointable outPointable = castResult.getOutPointable();
        ATypeTag reqTypeTag;
        if (oType == null || (reqTypeTag = oType.getTypeTag()) == ATypeTag.ANY) {
            // for open type case
            outPointable.set(val);
            return null;
        }
        byte[] valueBytes = val.getByteArray();
        int valueOffset = val.getStartOffset();
        ATypeTag inputTypeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(valueBytes[valueOffset]);
        if (needPromoteDemote(inputTypeTag, reqTypeTag)) {
            try {
                castBuffer.reset();
                ATypeHierarchy.convertNumericTypeByteArray(valueBytes, valueOffset, val.getLength(), reqTypeTag,
                        castBuffer.getDataOutput(), strictDemote);
                outPointable.set(castBuffer);
            } catch (HyracksDataException e) {
                throw e;
            } catch (IOException e) {
                throw new RuntimeDataException(ErrorCode.TYPE_CONVERT, sourceLoc, inputTypeTag, reqTypeTag);
            }

        } else {
            outPointable.set(val);
        }
        return null;
    }

    private boolean needPromoteDemote(ATypeTag inTag, ATypeTag outTag) {
        return inTag != outTag && inTag != ATypeTag.NULL && inTag != ATypeTag.MISSING;
    }
}
