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

package org.apache.asterix.runtime.evaluators.functions.vector;

import java.io.DataOutput;
import java.io.IOException;
import java.util.function.ToDoubleBiFunction;

import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.exceptions.ExceptionUtil;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.ConstantEvalFactory;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class VectorDistanceScalarEvaluator implements IScalarEvaluator {

    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    private final DataOutput dataOutput = resultStorage.getDataOutput();

    private final IPointable pointableLeft;
    private final IPointable pointableRight;
    private final IScalarEvaluator evaluatorLeft;
    private final IScalarEvaluator evaluatorRight;

    // Function ID, for error reporting.
    private final FunctionIdentifier funcId;
    private final SourceLocation sourceLoc;
    private final ToDoubleBiFunction<double[], double[]> distanceFunc;

    private final AMutableDouble aDouble = new AMutableDouble(-1);
    private final VectorListDecoder decoder;
    private final IEvaluatorContext ctx;

    private final ISerializerDeserializer<ADouble> doubleSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADOUBLE);

    private final ListAccessor listAccessorLeft = new ListAccessor();
    private final ListAccessor listAccessorRight = new ListAccessor();
    private final boolean isConstantLeft;
    private final boolean isConstantRight;
    private double[] vectorArgsLeft = new double[0];
    private double[] vectorArgsRight = new double[0];

    public VectorDistanceScalarEvaluator(IEvaluatorContext context, final IScalarEvaluatorFactory[] evaluatorFactories,
            FunctionIdentifier funcId, ToDoubleBiFunction<double[], double[]> distanceFunc, SourceLocation sourceLoc)
            throws HyracksDataException {
        this.distanceFunc = distanceFunc;
        this.funcId = funcId;
        this.sourceLoc = sourceLoc;
        this.decoder = new VectorListDecoder();
        this.ctx = context;

        pointableLeft = new VoidPointable();
        pointableRight = new VoidPointable();
        evaluatorLeft = evaluatorFactories[0].createScalarEvaluator(context);
        evaluatorRight = evaluatorFactories[1].createScalarEvaluator(context);

        boolean constantLeft = false;
        boolean constantRight = false;
        try {

            if (evaluatorFactories[0] instanceof ConstantEvalFactory) {
                vectorArgsLeft = decodeConstantVector(evaluatorLeft, pointableLeft, listAccessorLeft, vectorArgsLeft);
                constantLeft = vectorArgsLeft != null;
            }

            if (evaluatorFactories[1] instanceof ConstantEvalFactory) {
                vectorArgsRight =
                        decodeConstantVector(evaluatorRight, pointableRight, listAccessorRight, vectorArgsRight);
                constantRight = vectorArgsRight != null;
            }

        } catch (IOException e) {
            constantLeft = false;
            constantRight = false;
        }

        this.isConstantLeft = constantLeft;
        this.isConstantRight = constantRight;
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        resultStorage.reset();
        try {
            if (!isConstantLeft) {
                evaluatorLeft.evaluate(tuple, pointableLeft);
            }
            if (!isConstantRight) {
                evaluatorRight.evaluate(tuple, pointableRight);
            }

            if (PointableHelper.checkAndSetMissingOrNull(result, pointableLeft, pointableRight)) {
                return;
            }

            if (!isConstantLeft && !checkListTypeAndReset(result, listAccessorLeft, pointableLeft, 0)) {
                return;
            }
            if (!isConstantRight && !checkListTypeAndReset(result, listAccessorRight, pointableRight, 1)) {
                return;
            }

            if (listAccessorLeft.size() != listAccessorRight.size() || listAccessorLeft.size() == 0
                    || listAccessorRight.size() == 0) {
                ExceptionUtil.warnFunctionEvalFailed(ctx, sourceLoc, funcId,
                        "vector distance expects equal-length non-empty lists");
                PointableHelper.setNull(result);
                return;
            }

            vectorArgsLeft = isConstantLeft ? vectorArgsLeft
                    : decoder.createArrayFromList(listAccessorLeft,
                            decoder.ensureDoubleCapacity(vectorArgsLeft, listAccessorLeft.size()));
            vectorArgsRight = isConstantRight ? vectorArgsRight
                    : decoder.createArrayFromList(listAccessorRight,
                            decoder.ensureDoubleCapacity(vectorArgsRight, listAccessorRight.size()));

            double distanceCal = distanceFunc.applyAsDouble(vectorArgsLeft, vectorArgsRight);

            if (Double.isNaN(distanceCal)) {
                ExceptionUtil.warnFunctionEvalFailed(ctx, sourceLoc, funcId, "distance computed to NaN");
                PointableHelper.setNull(result);
                return;
            }
            writeResult(distanceCal, dataOutput);
            result.set(resultStorage);
        } catch (RuntimeDataException e) {
            ExceptionUtil.warnFunctionEvalFailed(ctx, sourceLoc, funcId, e.getMessage());
            PointableHelper.setNull(result);
        } catch (IOException e) {
            ExceptionUtil.warnFunctionEvalFailed(ctx, sourceLoc, funcId, e.getMessage());
            PointableHelper.setNull(result);
        }
    }

    protected void writeResult(double distance, DataOutput dataOutput) throws IOException {
        aDouble.setValue(distance);
        doubleSerde.serialize(aDouble, dataOutput);
    }

    private boolean checkListTypeAndReset(IPointable result, ListAccessor listAccessor, IPointable pointable,
            int argIdx) throws HyracksDataException {
        if (!decoder.checkListType(pointable)) {
            ExceptionUtil.warnTypeMismatch(ctx, sourceLoc, funcId, pointable.getByteArray()[pointable.getStartOffset()],
                    argIdx, new byte[] { ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG,
                            ATypeTag.SERIALIZED_UNORDEREDLIST_TYPE_TAG });
            PointableHelper.setNull(result);
            return false;
        }
        listAccessor.reset(pointable.getByteArray(), pointable.getStartOffset());
        return true;
    }

    private double[] decodeConstantVector(IScalarEvaluator evaluator, IPointable pointable, ListAccessor listAccessor,
            double[] cached) throws HyracksDataException, IOException {
        evaluator.evaluate(null, pointable);
        if (!decoder.checkListType(pointable)) {
            // Do not fail at open/compile time for constants; return NULL at runtime with warning instead.
            return null;
        }
        listAccessor.reset(pointable.getByteArray(), pointable.getStartOffset());
        if (listAccessor.size() == 0) {
            return null;
        }
        return decoder.createArrayFromList(listAccessor, decoder.ensureDoubleCapacity(cached, listAccessor.size()));
    }

}
