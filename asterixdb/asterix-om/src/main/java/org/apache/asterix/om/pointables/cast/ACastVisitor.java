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

    public ACastVisitor() {
        this(true);
    }

    public ACastVisitor(boolean strictDemote) {
        this.strictDemote = strictDemote;
    }

    @Override
    public Void visit(AListVisitablePointable accessor, Triple<IVisitablePointable, IAType, Boolean> arg)
            throws HyracksDataException {
        AListCaster caster = laccessorToCaster.get(accessor);
        if (caster == null) {
            caster = new AListCaster();
            laccessorToCaster.put(accessor, caster);
        }
        if (arg.second.getTypeTag().equals(ATypeTag.ANY)) {
            arg.second = accessor.ordered() ? DefaultOpenFieldType.NESTED_OPEN_AORDERED_LIST_TYPE
                    : DefaultOpenFieldType.NESTED_OPEN_AUNORDERED_LIST_TYPE;
        }
        caster.castList(accessor, arg.first, (AbstractCollectionType) arg.second, this);
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
        if (arg.second.getTypeTag().equals(ATypeTag.ANY)) {
            arg.second = DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE;
        }
        ARecordType resultType = (ARecordType) arg.second;
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
            } catch (IOException e1) {
                throw new HyracksDataException(
                        "Type mismatch: cannot cast the " + inputTypeTag + " type to the " + reqTypeTag + " type.");
            }

        }

        return null;
    }

    private boolean needPromote(ATypeTag tag0, ATypeTag tag1) {
        return !(tag0 == tag1 || tag0 == ATypeTag.NULL || tag0 == ATypeTag.MISSING);
    }

}
