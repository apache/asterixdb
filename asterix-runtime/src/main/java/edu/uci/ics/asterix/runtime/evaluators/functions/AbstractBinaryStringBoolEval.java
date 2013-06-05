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
import java.util.Arrays;

import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ABoolean;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public abstract class AbstractBinaryStringBoolEval implements ICopyEvaluator {

    private DataOutput dout;

    // allowed input types
    private final static byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();
    private final static byte SER_STRING_TYPE_TAG = ATypeTag.STRING.serialize();

    private ArrayBackedValueStorage array0 = new ArrayBackedValueStorage();
    private ArrayBackedValueStorage array1 = new ArrayBackedValueStorage();
    private ICopyEvaluator evalLeft;
    private ICopyEvaluator evalRight;
    private final FunctionIdentifier funcID;
    @SuppressWarnings({ "rawtypes" })
    private ISerializerDeserializer boolSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ABOOLEAN);

    public AbstractBinaryStringBoolEval(DataOutput dout, ICopyEvaluatorFactory evalLeftFactory,
            ICopyEvaluatorFactory evalRightFactory, FunctionIdentifier funcID) throws AlgebricksException {
        this.dout = dout;
        this.evalLeft = evalLeftFactory.createEvaluator(array0);
        this.evalRight = evalRightFactory.createEvaluator(array1);
        this.funcID = funcID;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
        array0.reset();
        evalLeft.evaluate(tuple);
        array1.reset();
        evalRight.evaluate(tuple);

        try {
            if (array0.getByteArray()[0] == SER_NULL_TYPE_TAG && array1.getByteArray()[0] == SER_NULL_TYPE_TAG) {
                boolSerde.serialize(ABoolean.TRUE, dout);
                return;
            } else if ((array0.getByteArray()[0] == SER_NULL_TYPE_TAG && array1.getByteArray()[0] == SER_STRING_TYPE_TAG)
                    || (array0.getByteArray()[0] == SER_STRING_TYPE_TAG && array1.getByteArray()[0] == SER_NULL_TYPE_TAG)) {
                boolSerde.serialize(ABoolean.FALSE, dout);
                return;
            } else if (array0.getByteArray()[0] != SER_STRING_TYPE_TAG
                    || array1.getByteArray()[0] != SER_STRING_TYPE_TAG) {
                throw new AlgebricksException(funcID.getName() + ": expects input type STRING or NULL, but got "
                        + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(array0.getByteArray()[0]) + " and "
                        + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(array1.getByteArray()[0]) + ")!");
            }
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }

        byte[] b1 = array0.getByteArray();
        byte[] b2 = array1.getByteArray();

        int lLen = array0.getLength();
        int rLen = array1.getLength();

        int lStart = array0.getStartOffset();
        int rStart = array1.getStartOffset();
        ABoolean res = compute(b1, lLen, lStart, b2, rLen, rStart, array0, array1) ? ABoolean.TRUE : ABoolean.FALSE;
        try {
            boolSerde.serialize(res, dout);
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
    }

    protected abstract boolean compute(byte[] lBytes, int lLen, int lStart, byte[] rBytes, int rLen, int rStart,
            ArrayBackedValueStorage array0, ArrayBackedValueStorage array1) throws AlgebricksException;

    protected String toRegex(AString pattern) {
        StringBuilder sb = new StringBuilder();
        String str = pattern.getStringValue();
        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            if (c == '\\' && (i < str.length() - 1) && (str.charAt(i + 1) == '_' || str.charAt(i + 1) == '%')) {
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

    private final static char[] reservedRegexChars = new char[] { '\\', '(', ')', '[', ']', '{', '}', '.', '^', '$',
            '*', '|' };
    static {
        Arrays.sort(reservedRegexChars);
    }
}
