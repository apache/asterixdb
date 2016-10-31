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

import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.runtime.exceptions.UnsupportedTypeException;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.TaggedValuePointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public abstract class AbstractComparisonEvaluator implements IScalarEvaluator {

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
            IScalarEvaluatorFactory evalRightFactory, IHyracksTaskContext context) throws HyracksDataException {
        this.evalLeft = evalLeftFactory.createScalarEvaluator(context);
        this.evalRight = evalRightFactory.createScalarEvaluator(context);
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        resultStorage.reset();

        // Evaluates input args.
        evalLeft.evaluate(tuple, argLeft);
        evalRight.evaluate(tuple, argRight);
        argLeft.getValue(outLeft);
        argRight.getValue(outRight);

        // checks whether we can apply >, >=, <, and <= to the given type since
        // these operations cannot be defined for certain types.
        if (isTotallyOrderable()) {
            checkTotallyOrderable();
        }

        // Checks whether two types are comparable
        if (comparabilityCheck()) {
            // Two types can be compared
            int r = compareResults();
            ABoolean b = getComparisonResult(r) ? ABoolean.TRUE : ABoolean.FALSE;
            serde.serialize(b, out);
        } else {
            // result:NULL - two types cannot be compared.
            nullSerde.serialize(ANull.NULL, out);
        }
        result.set(resultStorage);
    }

    protected abstract boolean isTotallyOrderable();

    protected abstract boolean getComparisonResult(int r);

    // checks whether we can apply >, >=, <, and <= operations to the given type since
    // these operations can not be defined for certain types.
    protected void checkTotallyOrderable() throws HyracksDataException {
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
                    throw new UnsupportedTypeException(ComparisonHelper.COMPARISON, argLeft.getTag());
                default:
                    return;
            }
        }
    }

    // checks whether two types are comparable
    protected boolean comparabilityCheck() {
        // Checks whether two types are comparable or not
        ATypeTag typeTag1 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argLeft.getTag());
        ATypeTag typeTag2 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argRight.getTag());

        // Are two types compatible, meaning that they can be compared? (e.g., compare between numeric types
        return ATypeHierarchy.isCompatible(typeTag1, typeTag2);
    }

    protected int compareResults() throws HyracksDataException {
        int result = ch.compare(EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argLeft.getTag()),
                EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argRight.getTag()), outLeft, outRight);
        return result;
    }

}
