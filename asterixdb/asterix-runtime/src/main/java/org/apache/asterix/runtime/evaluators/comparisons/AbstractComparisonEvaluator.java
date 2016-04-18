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

import java.io.DataOutput;

import org.apache.asterix.dataflow.data.nontagged.comparators.ABinaryComparator;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.BinaryComparatorConstant.ComparableResultCode;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.TaggedValuePointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public abstract class AbstractComparisonEvaluator implements IScalarEvaluator {

    protected enum ComparisonResult {
        LESS_THAN,
        EQUAL,
        GREATER_THAN,
        UNKNOWN
    }

    protected ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    protected DataOutput out = resultStorage.getDataOutput();
    protected TaggedValuePointable argLeft = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
    protected TaggedValuePointable argRight = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
    protected IPointable outLeft = VoidPointable.FACTORY.createPointable();
    protected IPointable outRight = VoidPointable.FACTORY.createPointable();
    protected IScalarEvaluator evalLeft;
    protected IScalarEvaluator evalRight;
    private ComparisonHelper ch = new ComparisonHelper();

    @SuppressWarnings("unchecked")
    protected ISerializerDeserializer<ABoolean> serde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ABOOLEAN);
    @SuppressWarnings("unchecked")
    protected ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ANULL);

    public AbstractComparisonEvaluator(IScalarEvaluatorFactory evalLeftFactory,
            IScalarEvaluatorFactory evalRightFactory, IHyracksTaskContext context) throws AlgebricksException {
        this.evalLeft = evalLeftFactory.createScalarEvaluator(context);
        this.evalRight = evalRightFactory.createScalarEvaluator(context);
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws AlgebricksException {
        resultStorage.reset();
        evalInputs(tuple);

        // checks whether we can apply >, >=, <, and <= to the given type since
        // these operations cannot be defined for certain types.
        if (isTotallyOrderable()) {
            checkTotallyOrderable();
        }

        // Checks whether two types are comparable
        switch (comparabilityCheck()) {
            case UNKNOWN:
                // result:UNKNOWN - NULL value found
                try {
                    nullSerde.serialize(ANull.NULL, out);
                    result.set(resultStorage);
                    return;
                } catch (HyracksDataException e) {
                    throw new AlgebricksException(e);
                }
            case FALSE:
                // result:FALSE - two types cannot be compared. Thus we return FALSE since this is equality comparison
                ABoolean b = ABoolean.FALSE;
                try {
                    serde.serialize(b, out);
                } catch (HyracksDataException e) {
                    throw new AlgebricksException(e);
                }
                break;
            case TRUE:
                // Two types can be compared
                ComparisonResult r = compareResults();
                ABoolean b1 = getComparisonResult(r) ? ABoolean.TRUE : ABoolean.FALSE;
                try {
                    serde.serialize(b1, out);
                } catch (HyracksDataException e) {
                    throw new AlgebricksException(e);
                }
                break;
            default:
                throw new AlgebricksException(
                        "Comparison cannot be processed. The return code from ComparabilityCheck is not correct.");
        }
        result.set(resultStorage);
    }

    protected abstract boolean isTotallyOrderable();

    protected abstract boolean getComparisonResult(ComparisonResult r);

    protected void evalInputs(IFrameTupleReference tuple) throws AlgebricksException {
        evalLeft.evaluate(tuple, argLeft);
        evalRight.evaluate(tuple, argRight);

        argLeft.getValue(outLeft);
        argRight.getValue(outRight);
    }

    // checks whether we can apply >, >=, <, and <= operations to the given type since
    // these operations can not be defined for certain types.
    protected void checkTotallyOrderable() throws AlgebricksException {
        if (argLeft.getLength() != 0) {
            ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argLeft.getTag());
            switch (typeTag) {
                case DURATION:
                case INTERVAL:
                case LINE:
                case POINT:
                case POINT3D:
                case POLYGON:
                case CIRCLE:
                case RECTANGLE:
                    throw new AlgebricksException(
                            "Comparison operations (GT, GE, LT, and LE) for the " + typeTag + " type are not defined.");
                default:
                    return;
            }
        }
    }

    // checks whether two types are comparable
    protected ComparableResultCode comparabilityCheck() {
        // just check TypeTags
        return ABinaryComparator.isComparable(argLeft.getTag(), argRight.getTag());
    }

    protected ComparisonResult compareResults() throws AlgebricksException {
        boolean isLeftNull = false;
        boolean isRightNull = false;
        ATypeTag typeTag1 = null;
        ATypeTag typeTag2 = null;

        if (outLeft.getLength() == 0) {
            isLeftNull = true;
        } else {
            typeTag1 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argLeft.getTag());
            if (typeTag1 == ATypeTag.NULL) {
                isLeftNull = true;
            }
        }
        if (outRight.getLength() == 0) {
            isRightNull = true;
        } else {
            typeTag2 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argRight.getTag());
            if (typeTag2 == ATypeTag.NULL) {
                isRightNull = true;
            }
        }

        if (isLeftNull || isRightNull) {
            return ComparisonResult.UNKNOWN;
        }

        int result = ch.compare(typeTag1, typeTag2, outLeft, outRight);
        if (result == 0) {
            return ComparisonResult.EQUAL;
        } else if (result < 0) {
            return ComparisonResult.LESS_THAN;
        } else {
            return ComparisonResult.GREATER_THAN;
        }
    }

}
