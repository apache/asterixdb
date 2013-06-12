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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.uci.ics.asterix.common.utils.UTF8CharSequence;
import edu.uci.ics.asterix.formats.nontagged.AqlBinaryComparatorFactoryProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.types.BuiltinType;
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

public class StringReplaceDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new StringReplaceDescriptor();
        }
    };

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) throws AlgebricksException {

        return new ICopyEvaluatorFactory() {

            private static final long serialVersionUID = 1L;

            @Override
            public ICopyEvaluator createEvaluator(IDataOutputProvider output) throws AlgebricksException {

                DataOutput dout = output.getDataOutput();

                return new AbstractTripleStringStringEval(dout, args[0], args[1], args[2],
                        AsterixBuiltinFunctions.STRING_REPLACE) {

                    private Pattern pattern = null;
                    private Matcher matcher = null;
                    private String replace;
                    private String strPattern = "";
                    private StringBuffer resultBuf = new StringBuffer();
                    private ByteArrayAccessibleOutputStream lastPattern = new ByteArrayAccessibleOutputStream();
                    private ByteArrayAccessibleOutputStream lastReplace = new ByteArrayAccessibleOutputStream();
                    private IBinaryComparator strComp = AqlBinaryComparatorFactoryProvider.INSTANCE
                            .getBinaryComparatorFactory(BuiltinType.ASTRING, true).createBinaryComparator();
                    private UTF8CharSequence carSeq = new UTF8CharSequence();
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<AString> stringSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ASTRING);

                    @Override
                    protected String compute(byte[] b0, int l0, int s0, byte[] b1, int l1, int s1, byte[] b2, int l2,
                            int s2, ArrayBackedValueStorage array0, ArrayBackedValueStorage array1)
                            throws AlgebricksException {
                        try {
                            boolean newPattern = false;
                            boolean newReplace = false;

                            AString astrPattern;
                            AString astrReplace;

                            if (pattern == null) {
                                newPattern = true;
                                newReplace = true;
                            } else {
                                int c = strComp.compare(b1, s1, l1, lastPattern.getByteArray(), 0, lastPattern.size());
                                if (c != 0) {
                                    newPattern = true;
                                }

                                c = strComp.compare(b2, s2, l2, lastReplace.getByteArray(), 0, lastReplace.size());
                                if (c != 0) {
                                    newReplace = true;
                                }
                            }
                            if (newPattern) {
                                lastPattern.reset();
                                lastPattern.write(b1, s1, l1);
                                // ! object creation !
                                DataInputStream di = new DataInputStream(new ByteArrayInputStream(
                                        lastPattern.getByteArray()));
                                astrPattern = (AString) stringSerde.deserialize(di);
                                // strPattern = toRegex(astrPattern);
                                strPattern = astrPattern.getStringValue();
                            }
                            if (newReplace) {
                                lastReplace.reset();
                                lastReplace.write(b2, s2, l2);
                                // ! object creation !
                                DataInputStream di = new DataInputStream(new ByteArrayInputStream(
                                        lastReplace.getByteArray()));
                                astrReplace = (AString) stringSerde.deserialize(di);
                                replace = astrReplace.getStringValue();
                            }
                            if (newPattern)
                                pattern = Pattern.compile(strPattern);
                            carSeq.reset(array0, 1);
                            if (newPattern) {
                                matcher = pattern.matcher(carSeq);
                            } else {
                                matcher.reset(carSeq);
                            }
                            while (matcher.find()) {
                                matcher.appendReplacement(resultBuf, replace);
                            }
                            matcher.appendTail(resultBuf);
                            return resultBuf.toString();
                        } catch (HyracksDataException e) {
                            throw new AlgebricksException(e);
                        }
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.STRING_REPLACE;
    }
}
