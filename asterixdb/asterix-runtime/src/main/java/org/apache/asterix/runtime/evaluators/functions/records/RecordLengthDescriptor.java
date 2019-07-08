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

package org.apache.asterix.runtime.evaluators.functions.records;

import java.io.DataOutput;

import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.functions.IFunctionTypeInferer;
import org.apache.asterix.om.pointables.nonvisitor.ARecordPointable;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.asterix.runtime.functions.FunctionTypeInferers;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

@MissingNullInOutFunction
public class RecordLengthDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new RecordLengthDescriptor();
        }

        @Override
        public IFunctionTypeInferer createFunctionTypeInferer() {
            return FunctionTypeInferers.RecordAccessorTypeInferer.INSTANCE_LAX;
        }
    };

    private static final long serialVersionUID = 1L;
    private ARecordType recType;

    @Override
    public void setImmutableStates(Object... states) {
        this.recType = (ARecordType) states[0];
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
                return new IScalarEvaluator() {

                    private IScalarEvaluator eval0 = args[0].createScalarEvaluator(ctx);

                    private final IPointable argPtr = new VoidPointable();
                    private final ARecordPointable recordPointable = ARecordPointable.FACTORY.createPointable();
                    private final AMutableInt64 aInt64 = new AMutableInt64(0);
                    @SuppressWarnings("unchecked")
                    private final ISerializerDeserializer<AInt64> int64Serde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT64);
                    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private final DataOutput out = resultStorage.getDataOutput();

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable resultPointable)
                            throws HyracksDataException {
                        resultStorage.reset();
                        eval0.evaluate(tuple, argPtr);

                        if (PointableHelper.checkAndSetMissingOrNull(resultPointable, argPtr)) {
                            return;
                        }

                        byte[] data = argPtr.getByteArray();
                        int offset = argPtr.getStartOffset();

                        if (data[offset] != ATypeTag.SERIALIZED_RECORD_TYPE_TAG) {
                            PointableHelper.setNull(resultPointable);
                            return;
                        }

                        recordPointable.set(data, offset, argPtr.getLength());
                        int n = recordPointable.getSchemeFieldCount(recType)
                                + recordPointable.getOpenFieldCount(recType);

                        aInt64.setValue(n);
                        int64Serde.serialize(aInt64, out);
                        resultPointable.set(resultStorage);
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.RECORD_LENGTH;
    }
}
