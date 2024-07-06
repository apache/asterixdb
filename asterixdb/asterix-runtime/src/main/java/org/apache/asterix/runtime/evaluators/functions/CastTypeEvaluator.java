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
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class CastTypeEvaluator implements IScalarEvaluator {

    private final IPointable argPointable = new VoidPointable();
    private final IObjectPool<AFlatValueCastingPointable, IAType> flatValuePool =
            new ListObjectPool<>(AFlatValueCastingPointable.FACTORY);
    private final IObjectPool<ARecordCastingPointable, IAType> recordValuePool =
            new ListObjectPool<>(ARecordCastingPointable.FACTORY);
    private final IObjectPool<AListCastingPointable, IAType> listValuePool =
            new ListObjectPool<>(AListCastingPointable.FACTORY);
    private IScalarEvaluator argEvaluator;
    protected final SourceLocation sourceLoc;
    private ICastingPointable inputPointable;
    private final ACastingPointableVisitor castVisitor = createCastVisitor();
    private final CastResult castResult = new CastResult(new VoidPointable(), null);
    private boolean inputTypeIsAny;

    public CastTypeEvaluator(SourceLocation sourceLoc) {
        this.sourceLoc = sourceLoc;
        // reset() should be called after using this constructor before calling any method
    }

    public CastTypeEvaluator(IAType reqType, IAType inputType, IScalarEvaluator argEvaluator,
            SourceLocation sourceLoc) {
        this.sourceLoc = sourceLoc;
        resetAndAllocate(reqType, inputType, argEvaluator);
    }

    public void resetAndAllocate(IAType reqType, IAType inputType, IScalarEvaluator argEvaluator) {
        this.argEvaluator = argEvaluator;
        this.inputPointable = allocatePointable(inputType);
        this.castResult.setOutType(reqType);
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

        cast(argPointable, result);
    }

    // TODO(ali): refactor in a better way
    protected void cast(IPointable argPointable, IPointable result) throws HyracksDataException {
        if (inputTypeIsAny) {
            ATypeTag inTag = EnumDeserializer.ATYPETAGDESERIALIZER
                    .deserialize(argPointable.getByteArray()[argPointable.getStartOffset()]);
            inputPointable = allocateForInput(TypeTagUtil.getBuiltinTypeByTag(inTag));
        }
        inputPointable.set(argPointable);
        castInto(result);
    }

    protected void castInto(IPointable result) throws HyracksDataException {
        inputPointable.accept(castVisitor, castResult);
        result.set(castResult.getOutPointable());
    }

    private ICastingPointable allocatePointable(IAType inputType) {
        if (!inputType.equals(BuiltinType.ANY)) {
            inputTypeIsAny = false;
            return allocateForInput(inputType);
        } else {
            inputTypeIsAny = true;
            return null;
        }
    }

    private ICastingPointable allocateForInput(IAType inputType) {
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
