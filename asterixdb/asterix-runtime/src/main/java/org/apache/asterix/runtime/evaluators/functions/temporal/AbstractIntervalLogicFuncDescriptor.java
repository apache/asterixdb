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

import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.pointables.nonvisitor.AIntervalPointable;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.TaggedValuePointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public abstract class AbstractIntervalLogicFuncDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private final static long serialVersionUID = 1L;

    /* (non-Javadoc)
     * @see org.apache.asterix.runtime.base.IScalarFunctionDynamicDescriptor#createEvaluatorFactory(org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory[])
     */
    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args)
            throws AlgebricksException {
        return new IScalarEvaluatorFactory() {

            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IHyracksTaskContext ctx) throws AlgebricksException {

                return new IScalarEvaluator() {

                    protected final IntervalLogic il = new IntervalLogic();
                    private ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private DataOutput out = resultStorage.getDataOutput();
                    private TaggedValuePointable argPtr0 = (TaggedValuePointable) TaggedValuePointable.FACTORY
                            .createPointable();
                    private TaggedValuePointable argPtr1 = (TaggedValuePointable) TaggedValuePointable.FACTORY
                            .createPointable();
                    private AIntervalPointable interval0 = (AIntervalPointable) AIntervalPointable.FACTORY
                            .createPointable();
                    private AIntervalPointable interval1 = (AIntervalPointable) AIntervalPointable.FACTORY
                            .createPointable();
                    private IScalarEvaluator eval0 = args[0].createScalarEvaluator(ctx);
                    private IScalarEvaluator eval1 = args[1].createScalarEvaluator(ctx);

                    // possible output types
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ANULL);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ABoolean> booleanSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ABOOLEAN);

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws AlgebricksException {
                        resultStorage.reset();
                        eval0.evaluate(tuple, argPtr0);
                        eval1.evaluate(tuple, argPtr1);

                        try {
                            if (argPtr0.getTag() == ATypeTag.SERIALIZED_NULL_TYPE_TAG
                                    || argPtr1.getTag() == ATypeTag.SERIALIZED_NULL_TYPE_TAG) {
                                nullSerde.serialize(ANull.NULL, out);
                                result.set(resultStorage);
                                return;
                            }

                            if (argPtr0.getTag() != ATypeTag.SERIALIZED_INTERVAL_TYPE_TAG
                                    || argPtr1.getTag() != ATypeTag.SERIALIZED_INTERVAL_TYPE_TAG) {
                                throw new AlgebricksException(getIdentifier().getName()
                                        + ": expects input type (INTERVAL, INTERVAL) but got ("
                                        + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argPtr0.getTag()) + ", "
                                        + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argPtr1.getTag()) + ")");
                            }

                            argPtr0.getValue(interval0);
                            argPtr1.getValue(interval1);

                            if (interval0.getType() != interval1.getType()) {
                                throw new AlgebricksException(getIdentifier().getName()
                                        + ": failed to compare intervals with different internal time type.");
                            }

                            ABoolean res = (compareIntervals(il, interval0, interval1)) ? ABoolean.TRUE : ABoolean.FALSE;

                            booleanSerde.serialize(res, out);
                        } catch (HyracksDataException hex) {
                            throw new AlgebricksException(hex);
                        }
                        result.set(resultStorage);
                    }
                };
            }
        };
    }

    protected abstract boolean compareIntervals(IntervalLogic il, AIntervalPointable ip1, AIntervalPointable ip2)
            throws AlgebricksException;

}
