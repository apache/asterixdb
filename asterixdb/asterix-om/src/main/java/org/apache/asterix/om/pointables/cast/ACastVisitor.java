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
import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.om.pointables.AFlatValuePointable;
import org.apache.asterix.om.pointables.AListVisitablePointable;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.pointables.visitor.IVisitablePointableVisitor;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

/**
 * This class is a IVisitablePointableVisitor implementation which recursively
 * visit a given record, list or flat value of a given type, and cast it to a
 * specified type. For example:
 * A record { "hobby": {{"music", "coding"}}, "id": "001", "name":
 * "Person Three"} which confirms to closed type ( id: string, name: string,
 * hobby: {{string}}? ) can be casted to a open type (id: string )
 * Since the open/closed part of a record has a completely different underlying
 * memory/storage layout, the visitor will change the layout as specified at
 * runtime.
 */
public class ACastVisitor implements IVisitablePointableVisitor<Void, Triple<IVisitablePointable, IAType, Boolean>> {

    private final Map<IVisitablePointable, ARecordCaster> raccessorToCaster = new HashMap<>();
    private final Map<IVisitablePointable, AListCaster> laccessorToCaster = new HashMap<>();
    private final ArrayBackedValueStorage castBuffer = new ArrayBackedValueStorage();

    private final boolean strictDemote;
    private final SourceLocation sourceLoc;

    public ACastVisitor() {
        this(true, null);
    }

    public ACastVisitor(SourceLocation sourceLoc) {
        this(true, sourceLoc);
    }

    public ACastVisitor(boolean strictDemote, SourceLocation sourceLoc) {
        this.strictDemote = strictDemote;
        this.sourceLoc = sourceLoc;
    }

    @Override
    public Void visit(AListVisitablePointable accessor, Triple<IVisitablePointable, IAType, Boolean> arg)
            throws HyracksDataException {
        AListCaster caster = laccessorToCaster.get(accessor);
        if (caster == null) {
            caster = new AListCaster();
            laccessorToCaster.put(accessor, caster);
        }

        AbstractCollectionType resultType;
        switch (arg.second.getTypeTag()) {
            case ANY:
                resultType = accessor.ordered() ? DefaultOpenFieldType.NESTED_OPEN_AORDERED_LIST_TYPE
                        : DefaultOpenFieldType.NESTED_OPEN_AUNORDERED_LIST_TYPE;
                break;
            case ARRAY:
            case MULTISET:
                resultType = (AbstractCollectionType) arg.second;
                break;
            default:
                throw new RuntimeDataException(ErrorCode.TYPE_CONVERT, sourceLoc,
                        accessor.ordered() ? ATypeTag.ARRAY : ATypeTag.MULTISET, arg.second.getTypeTag());
        }

        caster.castList(accessor, arg.first, resultType, this);
        return null;
    }

    @Override
    public Void visit(ARecordVisitablePointable accessor, Triple<IVisitablePointable, IAType, Boolean> arg)
            throws HyracksDataException {
        ARecordCaster caster = raccessorToCaster.get(accessor);
        if (caster == null) {
            caster = new ARecordCaster();
            raccessorToCaster.put(accessor, caster);
        }

        ARecordType resultType;
        switch (arg.second.getTypeTag()) {
            case ANY:
                resultType = DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE;
                break;
            case OBJECT:
                resultType = (ARecordType) arg.second;
                break;
            default:
                throw new RuntimeDataException(ErrorCode.TYPE_CONVERT, sourceLoc, ATypeTag.OBJECT,
                        arg.second.getTypeTag());
        }

        caster.castRecord(accessor, arg.first, resultType, this);
        return null;
    }

    @Override
    public Void visit(AFlatValuePointable accessor, Triple<IVisitablePointable, IAType, Boolean> arg)
            throws HyracksDataException {
        if (arg.second == null) {
            // for open type case
            arg.first.set(accessor);
            return null;
        }
        // set the pointer for result
        ATypeTag reqTypeTag = (arg.second).getTypeTag();
        if (reqTypeTag == ATypeTag.ANY) {
            // for open type case
            arg.first.set(accessor);
            return null;
        }
        ATypeTag inputTypeTag =
                EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(accessor.getByteArray()[accessor.getStartOffset()]);
        if (!needPromote(inputTypeTag, reqTypeTag)) {
            arg.first.set(accessor);
        } else {
            try {
                castBuffer.reset();
                ATypeHierarchy.convertNumericTypeByteArray(accessor.getByteArray(), accessor.getStartOffset(),
                        accessor.getLength(), reqTypeTag, castBuffer.getDataOutput(), strictDemote);
                arg.first.set(castBuffer);
            } catch (HyracksDataException e) {
                throw e;
            } catch (IOException e) {
                throw new RuntimeDataException(ErrorCode.TYPE_CONVERT, sourceLoc, inputTypeTag, reqTypeTag);
            }
        }

        return null;
    }

    private boolean needPromote(ATypeTag tag0, ATypeTag tag1) {
        return !(tag0 == tag1 || tag0 == ATypeTag.NULL || tag0 == ATypeTag.MISSING);
    }

}
