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
package org.apache.asterix.runtime.evaluators.comparisons;

import static org.apache.asterix.om.types.ATypeTag.VALUE_TYPE_MAPPING;

import java.io.DataOutput;

import org.apache.asterix.dataflow.data.common.ILogicalBinaryComparator;
import org.apache.asterix.dataflow.data.common.ILogicalBinaryComparator.Result;
import org.apache.asterix.dataflow.data.common.TaggedValueReference;
import org.apache.asterix.dataflow.data.nontagged.comparators.ComparatorUtil;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt8SerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.AFloat;
import org.apache.asterix.om.base.AInt16;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AInt8;
import org.apache.asterix.om.base.AMissing;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.ConstantEvalFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.TaggedValuePointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public abstract class AbstractComparisonEvaluator implements IScalarEvaluator {

    @SuppressWarnings("unchecked")
    protected final ISerializerDeserializer<AMissing> missingSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AMISSING);
    @SuppressWarnings("unchecked")
    protected final ISerializerDeserializer<ANull> nullSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ANULL);
    protected final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    protected final DataOutput out = resultStorage.getDataOutput();
    protected final TaggedValuePointable argLeft = TaggedValuePointable.FACTORY.createPointable();
    private final TaggedValuePointable argRight = TaggedValuePointable.FACTORY.createPointable();
    private final TaggedValueReference leftVal = new TaggedValueReference();
    private final TaggedValueReference rightVal = new TaggedValueReference();
    private final IScalarEvaluator evalLeft;
    private final IScalarEvaluator evalRight;
    protected final SourceLocation sourceLoc;
    private final ILogicalBinaryComparator logicalComparator;
    private IAObject leftConstant;
    private IAObject rightConstant;

    public AbstractComparisonEvaluator(IScalarEvaluatorFactory evalLeftFactory, IAType leftType,
            IScalarEvaluatorFactory evalRightFactory, IAType rightType, IEvaluatorContext ctx, SourceLocation sourceLoc,
            boolean isEquality) throws HyracksDataException {
        this.evalLeft = evalLeftFactory.createScalarEvaluator(ctx);
        this.evalRight = evalRightFactory.createScalarEvaluator(ctx);
        this.sourceLoc = sourceLoc;
        logicalComparator = ComparatorUtil.createLogicalComparator(leftType, rightType, isEquality);
        leftConstant = getValueOfConstantEval(evalLeftFactory);
        rightConstant = getValueOfConstantEval(evalRightFactory);
    }

    private IAObject getValueOfConstantEval(IScalarEvaluatorFactory factory) {
        if (factory instanceof ConstantEvalFactory) {
            return getConstantValue(((ConstantEvalFactory) factory).getValue());
        }
        return null;
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        // Evaluates input args.
        evalLeft.evaluate(tuple, argLeft);
        evalRight.evaluate(tuple, argRight);

        if (PointableHelper.checkAndSetMissingOrNull(result, argLeft, argRight)) {
            return;
        }
        leftVal.set(argLeft.getByteArray(), argLeft.getStartOffset() + 1, argLeft.getLength() - 1,
                VALUE_TYPE_MAPPING[argLeft.getTag()]);
        rightVal.set(argRight.getByteArray(), argRight.getStartOffset() + 1, argRight.getLength() - 1,
                VALUE_TYPE_MAPPING[argRight.getTag()]);
        evaluateImpl(result);
    }

    protected abstract void evaluateImpl(IPointable result) throws HyracksDataException;

    final Result compare() throws HyracksDataException {
        if (leftConstant != null) {
            if (rightConstant != null) {
                // both are constants
                return logicalComparator.compare(leftConstant, rightConstant);
            } else {
                // left is constant, right isn't
                return logicalComparator.compare(leftConstant, rightVal);
            }
        } else {
            if (rightConstant != null) {
                // right is constant, left isn't
                return logicalComparator.compare(leftVal, rightConstant);
            } else {
                return logicalComparator.compare(leftVal, rightVal);
            }
        }
    }

    final void writeMissing(IPointable result) throws HyracksDataException {
        resultStorage.reset();
        missingSerde.serialize(AMissing.MISSING, out);
        result.set(resultStorage);
    }

    final void writeNull(IPointable result) throws HyracksDataException {
        resultStorage.reset();
        nullSerde.serialize(ANull.NULL, out);
        result.set(resultStorage);
    }

    private IAObject getConstantValue(byte[] bytes) {
        int start = 0;
        ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes[start]);
        if (typeTag == null) {
            return null;
        }
        start++;
        switch (typeTag) {
            case TINYINT:
                return new AInt8(AInt8SerializerDeserializer.getByte(bytes, start));
            case SMALLINT:
                return new AInt16(AInt16SerializerDeserializer.getShort(bytes, start));
            case INTEGER:
                return new AInt32(AInt32SerializerDeserializer.getInt(bytes, start));
            case BIGINT:
                return new AInt64(AInt64SerializerDeserializer.getLong(bytes, start));
            case FLOAT:
                return new AFloat(AFloatSerializerDeserializer.getFloat(bytes, start));
            case DOUBLE:
                return new ADouble(ADoubleSerializerDeserializer.getDouble(bytes, start));
            default:
                return null;
        }
    }
}
