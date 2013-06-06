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

import java.io.DataOutput;
import java.util.Arrays;

import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.AMutableString;
import edu.uci.ics.asterix.om.base.ANull;
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

public abstract class AbstractQuadStringStringEval implements ICopyEvaluator {

    private DataOutput dout;
    private final static byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();
    private final static byte SER_STRING_TYPE_TAG = ATypeTag.STRING.serialize();
    private ArrayBackedValueStorage array0 = new ArrayBackedValueStorage();
    private ArrayBackedValueStorage array1 = new ArrayBackedValueStorage();
    private ArrayBackedValueStorage array2 = new ArrayBackedValueStorage();
    private ArrayBackedValueStorage array3 = new ArrayBackedValueStorage();
    private ICopyEvaluator eval0;
    private ICopyEvaluator eval1;
    private ICopyEvaluator eval2;
    private ICopyEvaluator eval3;

    private final FunctionIdentifier funcID;

    private AMutableString resultBuffer = new AMutableString("");
    @SuppressWarnings("rawtypes")
    private ISerializerDeserializer nullSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ANULL);
    @SuppressWarnings("rawtypes")
    private ISerializerDeserializer strSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ASTRING);

    public AbstractQuadStringStringEval(DataOutput dout, ICopyEvaluatorFactory eval0, ICopyEvaluatorFactory eval1,
            ICopyEvaluatorFactory eval2, ICopyEvaluatorFactory eval3, FunctionIdentifier funcID)
            throws AlgebricksException {
        this.dout = dout;
        this.eval0 = eval0.createEvaluator(array0);
        this.eval1 = eval1.createEvaluator(array1);
        this.eval2 = eval2.createEvaluator(array2);
        this.eval3 = eval3.createEvaluator(array3);
        this.funcID = funcID;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
        array0.reset();
        eval0.evaluate(tuple);
        array1.reset();
        eval1.evaluate(tuple);
        array2.reset();
        eval2.evaluate(tuple);
        array3.reset();
        eval3.evaluate(tuple);

        try {
            if (array0.getByteArray()[0] == SER_NULL_TYPE_TAG) {
                nullSerde.serialize(ANull.NULL, dout);
                return;
            } else if (array0.getByteArray()[0] == SER_STRING_TYPE_TAG) {
                if ((array1.getByteArray()[0] != SER_STRING_TYPE_TAG && array1.getByteArray()[0] != SER_NULL_TYPE_TAG)
                        || (array2.getByteArray()[0] != SER_STRING_TYPE_TAG && array2.getByteArray()[0] != SER_NULL_TYPE_TAG)
                        || (array3.getByteArray()[0] != SER_STRING_TYPE_TAG && array3.getByteArray()[0] != SER_NULL_TYPE_TAG)) {
                    throw new AlgebricksException(funcID.getName()
                            + ": expects input type (STRING/NULL, STRING/NULL, STRING/NULL, STRING/NULL), but got ("
                            + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(array0.getByteArray()[0]) + ", "
                            + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(array1.getByteArray()[0]) + ", "
                            + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(array2.getByteArray()[0]) + ", "
                            + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(array3.getByteArray()[0]) + ".");
                }

            } else {
                throw new AlgebricksException("Expects String or NULL Type.");
            }
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }

        byte[] b0 = array0.getByteArray();
        byte[] b1 = array1.getByteArray();
        byte[] b2 = array2.getByteArray();
        byte[] b3 = array3.getByteArray();

        int len0 = array0.getLength();
        int len1 = array1.getLength();
        int len2 = array2.getLength();
        int len3 = array3.getLength();

        int s0 = array0.getStartOffset();
        int s1 = array1.getStartOffset();
        int s2 = array2.getStartOffset();
        int s3 = array3.getStartOffset();

        String res = compute(b0, len0, s0, b1, len1, s1, b2, len2, s2, b3, len3, s3, array0, array1);
        resultBuffer.setValue(res);
        try {
            strSerde.serialize(resultBuffer, dout);
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
    }

    protected abstract String compute(byte[] b0, int l0, int s0, byte[] b1, int l1, int s1, byte[] b2, int l2, int s2,
            byte[] b3, int l3, int s3, ArrayBackedValueStorage array0, ArrayBackedValueStorage array1)
            throws AlgebricksException;

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
                if (Arrays.binarySearch(StringEvaluatorUtils.reservedRegexChars, c) >= 0) {
                    sb.append('\\');
                }
                sb.append(c);
            }
        }
        return sb.toString();
    }

}
