/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.runtime.evaluators.functions;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.uci.ics.asterix.common.utils.UTF8CharSequence;
import edu.uci.ics.asterix.formats.nontagged.AqlBinaryComparatorFactoryProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ABoolean;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.data.std.util.ByteArrayAccessibleOutputStream;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

/**
 * Creates new Matcher and Pattern objects each time the value of the pattern
 * argument (the second argument) changes.
 */

public class LikeDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;

    // allowed input types
    private final static byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();
    private final static byte SER_STRING_TYPE_TAG = ATypeTag.STRING.serialize();

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new LikeDescriptor();
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.LIKE;
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
                                throw new AlgebricksException(AsterixBuiltinFunctions.LIKE
                                        + ": expects input type STRING/NULL for the first argument but got "
                                        + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(array0.getByteArray()[0])
                                        + ".");
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
                                pattern = Pattern.compile(toRegex(strPattern));

                            }
                            array0.reset();
                            evalString.evaluate(tuple);
                            if (array0.getByteArray()[0] == SER_NULL_TYPE_TAG) {
                                nullSerde.serialize(ANull.NULL, dout);
                                return;
                            }
                            if (array0.getByteArray()[0] != SER_STRING_TYPE_TAG) {
                                throw new AlgebricksException(AsterixBuiltinFunctions.LIKE
                                        + ": expects input type (STRING/NULL, STRING/NULL) but got (STRING, "
                                        + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(array0.getByteArray()[0])
                                        + ").");
                            }
                            carSeq.reset(array0, 1);
                            if (newPattern) {
                                matcher = pattern.matcher(carSeq);
                            } else {
                                matcher.reset(carSeq);
                            }
                            ABoolean res = (matcher.matches()) ? ABoolean.TRUE : ABoolean.FALSE;

                            booleanSerde.serialize(res, dout);
                        } catch (HyracksDataException e) {
                            throw new AlgebricksException(e);
                        }
                    }

                    private String toRegex(AString pattern) {
                        StringBuilder sb = new StringBuilder();
                        String str = pattern.getStringValue();
                        for (int i = 0; i < str.length(); i++) {
                            char c = str.charAt(i);
                            if (c == '\\' && (i < str.length() - 1)
                                    && (str.charAt(i + 1) == '_' || str.charAt(i + 1) == '%')) {
                                sb.append(str.charAt(i + 1));
                                ++i;
                            } else if (c == '%') {
                                sb.append(".*");
                            } else if (c == '_') {
                                sb.append(".");
                            } else {
                                if (Arrays.binarySearch(reservedRegexChars, c) >= 0) {
                                    sb.append('\\');
                                }
                                sb.append(c);
                            }
                        }
                        return sb.toString();
                    }
                };
            }
        };
    }

    private static char[] reservedRegexChars = new char[] { '\\', '(', ')', '[', ']', '{', '}', '.', '^', '$', '*', '|' };
    static {
        Arrays.sort(reservedRegexChars);
    }
}
