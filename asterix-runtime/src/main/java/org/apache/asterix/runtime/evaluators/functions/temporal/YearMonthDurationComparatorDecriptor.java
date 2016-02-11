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

import org.apache.asterix.dataflow.data.nontagged.serde.ADurationSerializerDeserializer;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluator;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IDataOutputProvider;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class YearMonthDurationComparatorDecriptor extends AbstractScalarFunctionDynamicDescriptor {
    private final static long serialVersionUID = 1L;
    public final static FunctionIdentifier GREATER_THAN_FID = AsterixBuiltinFunctions.YEAR_MONTH_DURATION_GREATER_THAN;
    public final static FunctionIdentifier LESS_THAN_FID = AsterixBuiltinFunctions.YEAR_MONTH_DURATION_LESS_THAN;
    private final boolean isGreaterThan;

    private YearMonthDurationComparatorDecriptor(boolean isGreaterThan) {
        this.isGreaterThan = isGreaterThan;
    }

    public final static IFunctionDescriptorFactory GREATER_THAN_FACTORY = new IFunctionDescriptorFactory() {

        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new YearMonthDurationComparatorDecriptor(true);
        }
    };

    public final static IFunctionDescriptorFactory LESS_THAN_FACTORY = new IFunctionDescriptorFactory() {

        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new YearMonthDurationComparatorDecriptor(false);
        }
    };

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) throws AlgebricksException {
        return new ICopyEvaluatorFactory() {

            private static final long serialVersionUID = 1L;

            @Override
            public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
                return new ICopyEvaluator() {

                    private DataOutput out = output.getDataOutput();
                    private ArrayBackedValueStorage argOut0 = new ArrayBackedValueStorage();
                    private ArrayBackedValueStorage argOut1 = new ArrayBackedValueStorage();
                    private ICopyEvaluator eval0 = args[0].createEvaluator(argOut0);
                    private ICopyEvaluator eval1 = args[1].createEvaluator(argOut1);

                    // possible output types
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ANULL);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ABoolean> boolSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ABOOLEAN);

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        argOut0.reset();
                        eval0.evaluate(tuple);
                        argOut1.reset();
                        eval1.evaluate(tuple);

                        try {
                            if (argOut0.getByteArray()[0] == ATypeTag.SERIALIZED_NULL_TYPE_TAG
                                    || argOut1.getByteArray()[0] == ATypeTag.SERIALIZED_NULL_TYPE_TAG) {
                                nullSerde.serialize(ANull.NULL, out);
                                return;
                            }

                            if (argOut0.getByteArray()[0] != ATypeTag.SERIALIZED_DURATION_TYPE_TAG
                                    || argOut1.getByteArray()[0] != ATypeTag.SERIALIZED_DURATION_TYPE_TAG) {
                                throw new AlgebricksException(
                                        getIdentifier().getName()
                                                + ": expects type NULL/DURATION, NULL/DURATION but got "
                                                + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(
                                                        argOut0.getByteArray()[0])
                                                + " and " + EnumDeserializer.ATYPETAGDESERIALIZER
                                                        .deserialize(argOut1.getByteArray()[0]));
                            }

                            if ((ADurationSerializerDeserializer.getDayTime(argOut0.getByteArray(), 1) != 0)
                                    || (ADurationSerializerDeserializer.getDayTime(argOut1.getByteArray(), 1) != 0)) {
                                throw new AlgebricksException(
                                        getIdentifier().getName() + ": only year-month durations are allowed.");
                            }

                            if (ADurationSerializerDeserializer.getYearMonth(argOut0.getByteArray(),
                                    1) > ADurationSerializerDeserializer.getYearMonth(argOut1.getByteArray(), 1)) {
                                boolSerde.serialize(isGreaterThan ? ABoolean.TRUE : ABoolean.FALSE, out);
                            } else {
                                boolSerde.serialize(isGreaterThan ? ABoolean.FALSE : ABoolean.TRUE, out);
                            }

                        } catch (HyracksDataException hex) {
                            throw new AlgebricksException(hex);
                        }
                    }
                };
            }
        };
    }

    /* (non-Javadoc)
     * @see org.apache.asterix.om.functions.AbstractFunctionDescriptor#getIdentifier()
     */
    @Override
    public FunctionIdentifier getIdentifier() {
        return isGreaterThan ? GREATER_THAN_FID : LESS_THAN_FID;
    }

}
