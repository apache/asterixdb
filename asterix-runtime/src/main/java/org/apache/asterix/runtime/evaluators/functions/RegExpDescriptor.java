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
import org.apache.asterix.om.base.ANull;
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
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluator;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IDataOutputProvider;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
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

    // allowed input types
    private final static byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();
    private final static byte SER_STRING_TYPE_TAG = ATypeTag.STRING.serialize();

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new RegExpDescriptor();
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.REG_EXP;
    }

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) throws AlgebricksException {

        return new ICopyEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public ICopyEvaluator createEvaluator(IDataOutputProvider output) throws AlgebricksException {

                final DataOutput dout = output.getDataOutput();

                return new ICopyEvaluator() {

                    private boolean first = true;
                    private ArrayBackedValueStorage array0 = new ArrayBackedValueStorage();
                    private ICopyEvaluator evalString = args[0].createEvaluator(array0);
                    private ICopyEvaluator evalPattern = args[1].createEvaluator(array0);
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
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ANULL);
                    private Matcher matcher;
                    private Pattern pattern;

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        // evaluate the pattern first
                        try {
                            array0.reset();
                            evalPattern.evaluate(tuple);
                            if (array0.getByteArray()[0] == SER_NULL_TYPE_TAG) {
                                nullSerde.serialize(ANull.NULL, dout);
                                return;
                            }
                            if (array0.getByteArray()[0] != SER_STRING_TYPE_TAG) {
                                throw new AlgebricksException(AsterixBuiltinFunctions.REG_EXP.getName()
                                        + ": expects type STRING/NULL for the first input argument but got "
                                        + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(array0.getByteArray()[0]));
                            }
                            boolean newPattern = false;
                            if (first) {
                                first = false;
                                newPattern = true;
                            } else {
                                int c = strComp.compare(array0.getByteArray(), array0.getStartOffset(),
                                        array0.getLength(), lastPattern.getByteArray(), 0, lastPattern.size());
                                if (c != 0) {
                                    newPattern = true;
                                }
                            }
                            if (newPattern) {
                                lastPattern.reset();
                                lastPattern.write(array0.getByteArray(), array0.getStartOffset(), array0.getLength());
                                // ! object creation !
                                DataInputStream di = new DataInputStream(new ByteArrayInputStream(
                                        lastPattern.getByteArray()));
                                AString strPattern = (AString) stringSerde.deserialize(di);
                                pattern = Pattern.compile(strPattern.getStringValue());

                            }
                            array0.reset();
                            evalString.evaluate(tuple);
                            if (array0.getByteArray()[0] == SER_NULL_TYPE_TAG) {
                                nullSerde.serialize(ANull.NULL, dout);
                                return;
                            }
                            if (array0.getByteArray()[0] != SER_STRING_TYPE_TAG) {
                                throw new AlgebricksException(AsterixBuiltinFunctions.REG_EXP.getName()
                                        + ": expects type STRING/NULL for the second input argument but got "
                                        + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(array0.getByteArray()[0]));
                            }
                            utf8Ptr.set(array0.getByteArray(), 1, array0.getLength() - 1);
                            carSeq.reset(utf8Ptr);
                            if (newPattern) {
                                matcher = pattern.matcher(carSeq);
                            } else {
                                matcher.reset(carSeq);
                            }
                            ABoolean res = (matcher.find(0)) ? ABoolean.TRUE : ABoolean.FALSE;
                            booleanSerde.serialize(res, dout);
                        } catch (HyracksDataException e) {
                            throw new AlgebricksException(e);
                        }
                    }
                };
            }
        };
    }
}
