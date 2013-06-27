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

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class SubstringAfterDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;

    // allowed input types
    private static final byte SER_STRING_TYPE_TAG = ATypeTag.STRING.serialize();
    private static final byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new SubstringAfterDescriptor();
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
                    private ArrayBackedValueStorage array0 = new ArrayBackedValueStorage();
                    private ArrayBackedValueStorage array1 = new ArrayBackedValueStorage();
                    private ICopyEvaluator evalString = args[0].createEvaluator(array0);
                    private ICopyEvaluator evalPattern = args[1].createEvaluator(array1);
                    private final byte stt = ATypeTag.STRING.serialize();

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        array0.reset();
                        evalString.evaluate(tuple);
                        byte[] src = array0.getByteArray();

                        array1.reset();
                        evalPattern.evaluate(tuple);
                        byte[] pattern = array1.getByteArray();

                        if ((src[0] != SER_STRING_TYPE_TAG && src[0] != SER_NULL_TYPE_TAG)
                                || (pattern[0] != SER_STRING_TYPE_TAG && pattern[0] != SER_NULL_TYPE_TAG)) {
                            throw new AlgebricksException(AsterixBuiltinFunctions.SUBSTRING_AFTER.getName()
                                    + ": expects input type (STRING/NULL, STRING/NULL) but got ("
                                    + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(src[0]) + ", "
                                    + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(pattern[0]) + ").");
                        }

                        int srcLen = UTF8StringPointable.getUTFLength(src, 1);
                        int patternLen = UTF8StringPointable.getUTFLength(pattern, 1);
                        int posSrc = 3;
                        int posPattern = 3;

                        int offset = 0;
                        // boolean found = false;
                        while (posSrc - 3 < srcLen - patternLen) {
                            offset = 0;
                            while (posPattern + offset - 3 < patternLen && posSrc + offset - 3 < srcLen) {
                                char c1 = UTF8StringPointable.charAt(src, posSrc + offset);
                                char c2 = UTF8StringPointable.charAt(pattern, posPattern + offset);
                                if (c1 != c2)
                                    break;
                                offset++;
                            }
                            if (offset == patternLen) {
                                // found = true;
                                break;
                            }
                            posSrc += UTF8StringPointable.charSize(src, posSrc);
                        }

                        posSrc += patternLen;
                        int substrByteLen = srcLen - posSrc + 3;
                        try {
                            out.writeByte(stt);
                            out.writeByte((byte) ((substrByteLen >>> 8) & 0xFF));
                            out.writeByte((byte) ((substrByteLen >>> 0) & 0xFF));
                            out.write(src, posSrc, substrByteLen);

                        } catch (IOException e) {
                            throw new AlgebricksException(e);
                        }
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.SUBSTRING_AFTER;
    }

}
