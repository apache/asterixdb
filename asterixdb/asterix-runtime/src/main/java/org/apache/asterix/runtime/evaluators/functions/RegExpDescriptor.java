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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.asterix.formats.nontagged.AqlBinaryComparatorFactoryProvider;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleOutputStream;
import org.apache.hyracks.data.std.util.UTF8CharSequence;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

/**
 * Creates new Matcher and Pattern objects each time the value of the pattern
 * argument (the second argument) changes.
 */

public class RegExpDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new RegExpDescriptor();
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.REG_EXP;
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args)
            throws AlgebricksException {

        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(IHyracksTaskContext ctx) throws AlgebricksException {

                return new IScalarEvaluator() {

                    private ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private DataOutput dout = resultStorage.getDataOutput();
                    private boolean first = true;
                    private IPointable array0 = new VoidPointable();
                    private IScalarEvaluator evalString = args[0].createScalarEvaluator(ctx);
                    private IScalarEvaluator evalPattern = args[1].createScalarEvaluator(ctx);
                    private ByteArrayAccessibleOutputStream lastPattern = new ByteArrayAccessibleOutputStream();
                    private UTF8CharSequence carSeq = new UTF8CharSequence();
                    private UTF8StringPointable utf8Ptr = new UTF8StringPointable();
                    private IBinaryComparator strComp = AqlBinaryComparatorFactoryProvider.INSTANCE
                            .getBinaryComparatorFactory(BuiltinType.ASTRING, true).createBinaryComparator();
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<AString> stringSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ASTRING);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ABoolean> booleanSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ABOOLEAN);
                    private Matcher matcher;
                    private Pattern pattern;

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws AlgebricksException {
                        resultStorage.reset();
                        // evaluate the pattern first
                        try {
                            evalPattern.evaluate(tuple, array0);
                            evalString.evaluate(tuple, array0);

                            byte[] patternBytes = array0.getByteArray();
                            int patternOffset = array0.getStartOffset();
                            int patternLen = array0.getLength();
                            if (patternBytes[patternOffset] != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
                                throw new AlgebricksException(AsterixBuiltinFunctions.REG_EXP.getName()
                                        + ": expects type STRING/NULL for the first input argument but got "
                                        + EnumDeserializer.ATYPETAGDESERIALIZER
                                                .deserialize(patternBytes[patternOffset]));
                            }
                            boolean newPattern = false;
                            if (first) {
                                first = false;
                                newPattern = true;
                            } else {
                                int c = strComp.compare(patternBytes, patternOffset, patternLen,
                                        lastPattern.getByteArray(), 0, lastPattern.size());
                                if (c != 0) {
                                    newPattern = true;
                                }
                            }
                            if (newPattern) {
                                lastPattern.reset();
                                lastPattern.write(patternBytes, patternOffset, patternLen);
                                // ! object creation !
                                DataInputStream di = new DataInputStream(
                                        new ByteArrayInputStream(lastPattern.getByteArray()));
                                AString strPattern = stringSerde.deserialize(di);
                                pattern = Pattern.compile(strPattern.getStringValue());

                            }
                            byte[] data = array0.getByteArray();
                            int offset = array0.getStartOffset();
                            int len = array0.getLength();
                            if (data[offset] != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
                                throw new AlgebricksException(AsterixBuiltinFunctions.REG_EXP.getName()
                                        + ": expects type STRING/NULL for the second input argument but got "
                                        + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(data[offset]));
                            }
                            utf8Ptr.set(data, offset + 1, len - 1);
                            carSeq.reset(utf8Ptr);
                            if (newPattern) {
                                matcher = pattern.matcher(carSeq);
                            } else {
                                matcher.reset(carSeq);
                            }
                            ABoolean res = (matcher.find(0)) ? ABoolean.TRUE : ABoolean.FALSE;
                            booleanSerde.serialize(res, dout);
                            result.set(resultStorage);
                        } catch (HyracksDataException e) {
                            throw new AlgebricksException(e);
                        }
                    }
                };
            }
        };
    }
}
