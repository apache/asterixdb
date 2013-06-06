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
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
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

public class StringMatchesWithFlagDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new StringMatchesWithFlagDescriptor();
        }
    };

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) throws AlgebricksException {

        return new ICopyEvaluatorFactory() {

            private static final long serialVersionUID = 1L;

            @Override
            public ICopyEvaluator createEvaluator(IDataOutputProvider output) throws AlgebricksException {

                DataOutput dout = output.getDataOutput();

                return new AbstractTripleStringBoolEval(dout, args[0], args[1], args[2],
                        AsterixBuiltinFunctions.STRING_MATCHES_WITH_FLAG) {

                    private Pattern pattern = null;
                    private Matcher matcher = null;
                    private String strPattern = "";
                    private int flags = 0;
                    private ByteArrayAccessibleOutputStream lastPattern = new ByteArrayAccessibleOutputStream();
                    private ByteArrayAccessibleOutputStream lastFlags = new ByteArrayAccessibleOutputStream();
                    private IBinaryComparator strComp = AqlBinaryComparatorFactoryProvider.INSTANCE
                            .getBinaryComparatorFactory(BuiltinType.ASTRING, true).createBinaryComparator();
                    private UTF8CharSequence carSeq = new UTF8CharSequence();
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<AString> stringSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ASTRING);

                    @Override
                    protected boolean compute(byte[] b0, int l0, int s0, byte[] b1, int l1, int s1, byte[] b2, int l2,
                            int s2, ArrayBackedValueStorage array0, ArrayBackedValueStorage array1)
                            throws AlgebricksException {
                        try {
                            boolean newPattern = false;
                            boolean newFlags = false;

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

                                c = strComp.compare(b2, s2, l2, lastFlags.getByteArray(), 0, lastFlags.size());
                                if (c != 0) {
                                    newFlags = true;
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
                            if (newFlags) {
                                lastFlags.reset();
                                lastFlags.write(b2, s2, l2);
                                // ! object creation !
                                DataInputStream di = new DataInputStream(new ByteArrayInputStream(
                                        lastFlags.getByteArray()));
                                astrFlags = (AString) stringSerde.deserialize(di);
                                flags = StringEvaluatorUtils.toFlag(astrFlags);
                            }

                            pattern = Pattern.compile(strPattern, flags);
                            carSeq.reset(array0, 1);
                            if (newPattern) {
                                matcher = pattern.matcher(carSeq);
                            } else {
                                matcher.reset(carSeq);
                            }
                            return matcher.find();
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
        return AsterixBuiltinFunctions.STRING_MATCHES_WITH_FLAG;
    }
}
