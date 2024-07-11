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
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class CastTypeEvaluator implements IScalarEvaluator {

    private final IPointable argPointable = new VoidPointable();
    private final IScalarEvaluator argEvaluator;
    protected final SourceLocation sourceLoc;
    private ICastingPointable inputPointable;
    private ICastingPointable record;
    private ICastingPointable list;
    private ICastingPointable flat;
    private final ACastingPointableVisitor castVisitor = createCastVisitor();
    private final CastResult castResult = new CastResult(new VoidPointable(), null);
    private final boolean inputTypeIsAny;

    public CastTypeEvaluator(IAType reqType, IAType inputType, IScalarEvaluator argEvaluator,
            SourceLocation sourceLoc) {
        this.sourceLoc = sourceLoc;
        this.argEvaluator = argEvaluator;
        this.castResult.setOutType(reqType);
        if (!inputType.equals(BuiltinType.ANY)) {
            this.inputPointable = createPointable(inputType);
            this.inputTypeIsAny = false;
        } else {
            this.inputTypeIsAny = true;
        }
    }

    protected ACastingPointableVisitor createCastVisitor() {
        return ACastingPointableVisitor.strictCasting(sourceLoc);
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        argEvaluator.evaluate(tuple, argPointable);
        if (PointableHelper.checkAndSetMissingOrNull(result, argPointable)) {
            return;
        }
        if (inputTypeIsAny) {
            inputPointable = getPointable(argPointable);
        }
        inputPointable.set(argPointable);
        castInto(result);
    }

    protected void castInto(IPointable result) throws HyracksDataException {
        inputPointable.accept(castVisitor, castResult);
        result.set(castResult.getOutPointable());
    }

    private static ICastingPointable createPointable(IAType type) {
        switch (type.getTypeTag()) {
            case OBJECT:
                return ARecordCastingPointable.FACTORY.create(type);
            case ARRAY:
            case MULTISET:
                return AListCastingPointable.FACTORY.create(type);
            default:
                return AFlatValueCastingPointable.FACTORY.create(type);
        }
    }

    private ICastingPointable getPointable(IPointable arg) throws HyracksDataException {
        ATypeTag tag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(arg.getByteArray()[arg.getStartOffset()]);
        switch (tag) {
            case OBJECT:
                if (record == null) {
                    record = ARecordCastingPointable.FACTORY.create(TypeTagUtil.getBuiltinTypeByTag(tag));
                }
                return record;
            case ARRAY:
            case MULTISET:
                if (list == null) {
                    list = AListCastingPointable.FACTORY.create(TypeTagUtil.getBuiltinTypeByTag(tag));
                }
                return list;
            default:
                if (flat == null) {
                    flat = AFlatValueCastingPointable.FACTORY.create(null);
                }
                return flat;
        }
    }
}
