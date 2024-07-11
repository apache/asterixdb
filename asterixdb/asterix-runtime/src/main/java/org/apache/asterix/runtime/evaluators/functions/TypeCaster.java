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

package org.apache.asterix.runtime.evaluators.functions;

import org.apache.asterix.om.pointables.AFlatValueCastingPointable;
import org.apache.asterix.om.pointables.AListCastingPointable;
import org.apache.asterix.om.pointables.ARecordCastingPointable;
import org.apache.asterix.om.pointables.base.ICastingPointable;
import org.apache.asterix.om.pointables.cast.ACastingPointableVisitor;
import org.apache.asterix.om.pointables.cast.CastResult;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.TypeTagUtil;
import org.apache.asterix.om.util.container.IObjectPool;
import org.apache.asterix.om.util.container.ListObjectPool;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;

public class TypeCaster {

    private final IObjectPool<AFlatValueCastingPointable, IAType> flatValuePool =
            new ListObjectPool<>(AFlatValueCastingPointable.FACTORY);
    private final IObjectPool<ARecordCastingPointable, IAType> recordValuePool =
            new ListObjectPool<>(ARecordCastingPointable.FACTORY);
    private final IObjectPool<AListCastingPointable, IAType> listValuePool =
            new ListObjectPool<>(AListCastingPointable.FACTORY);
    private final CastResult castResult = new CastResult(new VoidPointable(), null);
    private final ACastingPointableVisitor castVisitor;

    public TypeCaster(SourceLocation sourceLoc) {
        this.castVisitor = new ACastingPointableVisitor(true, sourceLoc);
    }

    protected void allocateAndCast(IPointable argPointable, IAType argType, IPointable result, IAType reqType)
            throws HyracksDataException {
        castResult.setOutType(reqType);
        ICastingPointable inputPointable = allocate(argPointable, argType);
        inputPointable.accept(castVisitor, castResult);
        result.set(castResult.getOutPointable());
    }

    private ICastingPointable allocate(IPointable arg, IAType argType) throws HyracksDataException {
        ICastingPointable p;
        if (!argType.equals(BuiltinType.ANY)) {
            p = allocate(argType);
        } else {
            ATypeTag tag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(arg.getByteArray()[arg.getStartOffset()]);
            p = allocate(TypeTagUtil.getBuiltinTypeByTag(tag));
        }
        p.set(arg);
        return p;
    }

    private ICastingPointable allocate(IAType inputType) {
        ICastingPointable pointable;
        switch (inputType.getTypeTag()) {
            case OBJECT:
                pointable = recordValuePool.allocate(inputType);
                break;
            case ARRAY:
            case MULTISET:
                pointable = listValuePool.allocate(inputType);
                break;
            default:
                pointable = flatValuePool.allocate(null);
        }
        return pointable;
    }

    public void deallocatePointables() {
        flatValuePool.reset();
        listValuePool.reset();
        recordValuePool.reset();
    }
}
