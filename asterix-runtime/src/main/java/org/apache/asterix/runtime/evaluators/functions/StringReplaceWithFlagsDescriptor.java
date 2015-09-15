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

import org.apache.asterix.common.utils.UTF8CharSequence;
import org.apache.asterix.formats.nontagged.AqlBinaryComparatorFactoryProvider;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluator;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IDataOutputProvider;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleOutputStream;

public class StringReplaceWithFlagsDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new StringReplaceWithFlagsDescriptor();
        }
    };

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) throws AlgebricksException {

        return new ICopyEvaluatorFactory() {

            private static final long serialVersionUID = 1L;

            @Override
            public ICopyEvaluator createEvaluator(IDataOutputProvider output) throws AlgebricksException {

                DataOutput dout = output.getDataOutput();

                return new AbstractQuadStringStringEval(dout, args[0], args[1], args[2], args[3],
                        AsterixBuiltinFunctions.STRING_REPLACE_WITH_FLAG) {

                    private Pattern pattern = null;
                    private Matcher matcher = null;
                    private String strPattern = "";
                    private String replace = "";
                    private int flags = 0;
                    private StringBuffer resultBuf = new StringBuffer();
                    private ByteArrayAccessibleOutputStream lastPattern = new ByteArrayAccessibleOutputStream();
                    private ByteArrayAccessibleOutputStream lastFlags = new ByteArrayAccessibleOutputStream();
                    private ByteArrayAccessibleOutputStream lastReplace = new ByteArrayAccessibleOutputStream();
                    private IBinaryComparator strComp = AqlBinaryComparatorFactoryProvider.INSTANCE
                            .getBinaryComparatorFactory(BuiltinType.ASTRING, true).createBinaryComparator();
                    private UTF8CharSequence carSeq = new UTF8CharSequence();
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<AString> stringSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ASTRING);

                    @Override
                    protected String compute(byte[] b0, int l0, int s0, byte[] b1, int l1, int s1, byte[] b2, int l2,
                            int s2, byte[] b3, int l3, int s3, ArrayBackedValueStorage array0,
                            ArrayBackedValueStorage array1) throws AlgebricksException {
                        try {
                            boolean newPattern = false;
                            boolean newFlags = false;
                            boolean newReplace = false;

                            AString astrPattern;
                            AString astrFlags;

                            if (pattern == null) {
                                newPattern = true;
                                newFlags = true;
                            } else {
                                int c = strComp.compare(b1, s1, l1, lastPattern.getByteArray(), 0, lastPattern.size());
                                if (c != 0) {
                                    newPattern = true;
                                }

                                c = strComp.compare(b3, s3, l3, lastFlags.getByteArray(), 0, lastFlags.size());
                                if (c != 0) {
                                    newFlags = true;
                                }
                            }

                            if (replace == null) {
                                newReplace = true;
                            } else {
                                int c = strComp.compare(b2, s2, l2, lastReplace.getByteArray(), 0, lastReplace.size());
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
                                replace = ((AString) stringSerde.deserialize(di)).getStringValue();
                            }
                            if (newFlags) {
                                lastFlags.reset();
                                lastFlags.write(b3, s3, l3);
                                // ! object creation !
                                DataInputStream di = new DataInputStream(new ByteArrayInputStream(
                                        lastFlags.getByteArray()));
                                astrFlags = (AString) stringSerde.deserialize(di);
                                flags = StringEvaluatorUtils.toFlag(astrFlags);
                            }

                            if (newPattern || newFlags)
                                pattern = Pattern.compile(strPattern, flags);
                            resultBuf.setLength(0);
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
        return AsterixBuiltinFunctions.STRING_REPLACE_WITH_FLAG;
    }
}
