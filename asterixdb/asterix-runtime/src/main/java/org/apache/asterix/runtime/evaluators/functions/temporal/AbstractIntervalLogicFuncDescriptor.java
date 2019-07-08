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
package org.apache.asterix.runtime.evaluators.functions.temporal;

import java.io.DataOutput;

import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.pointables.nonvisitor.AIntervalPointable;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.TaggedValuePointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public abstract class AbstractIntervalLogicFuncDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {

                return new IScalarEvaluator() {

                    private final IntervalLogic il = new IntervalLogic();
                    private ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private DataOutput out = resultStorage.getDataOutput();
                    private TaggedValuePointable argPtr0 = TaggedValuePointable.FACTORY.createPointable();
                    private TaggedValuePointable argPtr1 = TaggedValuePointable.FACTORY.createPointable();
                    private AIntervalPointable interval0 =
                            (AIntervalPointable) AIntervalPointable.FACTORY.createPointable();
                    private AIntervalPointable interval1 =
                            (AIntervalPointable) AIntervalPointable.FACTORY.createPointable();
                    private IScalarEvaluator eval0 = args[0].createScalarEvaluator(ctx);
                    private IScalarEvaluator eval1 = args[1].createScalarEvaluator(ctx);

                    // possible output types
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ABoolean> booleanSerde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ABOOLEAN);

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        resultStorage.reset();
                        eval0.evaluate(tuple, argPtr0);
                        eval1.evaluate(tuple, argPtr1);
                        if (PointableHelper.checkAndSetMissingOrNull(result, argPtr0, argPtr1)) {
                            return;
                        }
                        byte typeTag0 = argPtr0.getTag();
                        if (typeTag0 != ATypeTag.SERIALIZED_INTERVAL_TYPE_TAG) {
                            throw new TypeMismatchException(sourceLoc, getIdentifier(), 0, typeTag0,
                                    ATypeTag.SERIALIZED_INTERVAL_TYPE_TAG);
                        }
                        byte typeTag1 = argPtr1.getTag();
                        if (typeTag1 != ATypeTag.SERIALIZED_INTERVAL_TYPE_TAG) {
                            throw new TypeMismatchException(sourceLoc, getIdentifier(), 1, typeTag1,
                                    ATypeTag.SERIALIZED_INTERVAL_TYPE_TAG);
                        }
                        argPtr0.getValue(interval0);
                        argPtr1.getValue(interval1);
                        ABoolean res = compareIntervals(il, interval0, interval1) ? ABoolean.TRUE : ABoolean.FALSE;
                        booleanSerde.serialize(res, out);
                        result.set(resultStorage);
                    }
                };
            }
        };
    }

    protected abstract boolean compareIntervals(IntervalLogic il, AIntervalPointable ip1, AIntervalPointable ip2)
            throws HyracksDataException;

}
