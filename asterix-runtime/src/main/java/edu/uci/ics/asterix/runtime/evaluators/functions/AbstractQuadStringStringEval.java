/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.uci.ics.asterix.runtime.evaluators.functions;

import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.AMutableString;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * @author ilovesoup
 */
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

    private AMutableString resultBuffer = new AMutableString("");
    @SuppressWarnings("rawtypes")
    private ISerializerDeserializer nullSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ANULL);
    @SuppressWarnings("rawtypes")
    private ISerializerDeserializer strSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ASTRING);

    public AbstractQuadStringStringEval(DataOutput dout, ICopyEvaluatorFactory eval0, ICopyEvaluatorFactory eval1,
            ICopyEvaluatorFactory eval2, ICopyEvaluatorFactory eval3) throws AlgebricksException {
        this.dout = dout;
        this.eval0 = eval0.createEvaluator(array0);
        this.eval1 = eval1.createEvaluator(array1);
        this.eval2 = eval2.createEvaluator(array2);
        this.eval3 = eval3.createEvaluator(array3);
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
                if (array0.getByteArray()[1] == SER_NULL_TYPE_TAG) {
                    dout.write(array0.getByteArray(), array0.getStartOffset(), array0.getLength());
                    return;
                }

            } else {
                throw new AlgebricksException("Expects String Type.");
            }
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        } catch (IOException e) {
            throw new AlgebricksException(e.getMessage());
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
                if (Arrays.binarySearch(reservedRegexChars, c) >= 0) {
                    sb.append('\\');
                }
                sb.append(c);
            }
        }
        return sb.toString();
    }

    protected int toFlag(AString pattern) {
        String str = pattern.getStringValue();
        int flag = 0;
        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            switch (c) {
                case 's':
                    flag |= Pattern.DOTALL;
                    break;
                case 'm':
                    flag |= Pattern.MULTILINE;
                    break;
                case 'i':
                    flag |= Pattern.CASE_INSENSITIVE;
                    break;
                case 'x':
                    flag |= Pattern.COMMENTS;
                    break;
            }
        }
        return flag;
    }

    private final static char[] reservedRegexChars = new char[] { '\\', '(', ')', '[', ']', '{', '}', '.', '^', '$',
            '*', '|' };

    static {
        Arrays.sort(reservedRegexChars);
    }
}
