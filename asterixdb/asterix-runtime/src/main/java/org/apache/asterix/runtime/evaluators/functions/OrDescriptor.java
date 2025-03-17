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

import java.io.DataOutput;

import org.apache.asterix.dataflow.data.nontagged.serde.ABooleanSerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.AMissing;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class OrDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = OrDescriptor::new;

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.OR;
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
                final IPointable argPtr = new VoidPointable();
                final IScalarEvaluator[] evals = new IScalarEvaluator[args.length];
                for (int i = 0; i < evals.length; i++) {
                    evals[i] = args[i].createScalarEvaluator(ctx);
                }

                return new IScalarEvaluator() {
                    private ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private DataOutput output = resultStorage.getDataOutput();
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ABoolean> booleanSerde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ABOOLEAN);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ANull> nullSerde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ANULL);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<AMissing> missingSerde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AMISSING);

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        resultStorage.reset();
                        int n = args.length;
                        boolean res = false;
                        boolean metNull = false;
                        boolean metMissing = false;
                        for (int i = 0; i < n; i++) {
                            evals[i].evaluate(tuple, argPtr);
                            byte[] data = argPtr.getByteArray();
                            int offset = argPtr.getStartOffset();
                            if (data[offset] == ATypeTag.SERIALIZED_MISSING_TYPE_TAG) {
                                metMissing = true;
                                continue;
                            }
                            if (data[offset] == ATypeTag.SERIALIZED_NULL_TYPE_TAG) {
                                metNull = true;
                                continue;
                            }
                            if (data[offset] != ATypeTag.SERIALIZED_BOOLEAN_TYPE_TAG) {
                                throw new TypeMismatchException(sourceLoc, getIdentifier(), i, data[offset],
                                        ATypeTag.SERIALIZED_BOOLEAN_TYPE_TAG);
                            }
                            boolean argResult = ABooleanSerializerDeserializer.getBoolean(data, offset + 1);
                            if (argResult) {
                                // anything OR TRUE = TRUE
                                booleanSerde.serialize(ABoolean.TRUE, output);
                                result.set(resultStorage);
                                return;
                            }
                            res |= argResult;
                        }
                        if (metNull) {
                            // NULL OR FALSE = NULL
                            // NULL OR MISSING = NULL
                            nullSerde.serialize(ANull.NULL, output);
                        } else if (metMissing) {
                            // MISSING OR FALSE = MISSING
                            missingSerde.serialize(AMissing.MISSING, output);
                        } else {
                            ABoolean aResult = res ? ABoolean.TRUE : ABoolean.FALSE;
                            booleanSerde.serialize(aResult, output);
                        }
                        result.set(resultStorage);
                    }
                };
            }
        };
    }

}
