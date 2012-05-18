package edu.uci.ics.asterix.runtime.evaluators.functions;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.common.functions.FunctionConstants;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.IEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IDataOutputProvider;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;

public class SubstringDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    private final static FunctionIdentifier FID = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "substring", 3,
            true);
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new SubstringDescriptor();
        }
    };

    @Override
    public IEvaluatorFactory createEvaluatorFactory(final IEvaluatorFactory[] args) throws AlgebricksException {
        return new IEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
                return new IEvaluator() {

                    private DataOutput out = output.getDataOutput();
                    private ArrayBackedValueStorage argOut = new ArrayBackedValueStorage();
                    private IEvaluator evalString = args[0].createEvaluator(argOut);
                    private IEvaluator evalStart = args[1].createEvaluator(argOut);
                    private IEvaluator evalLen = args[2].createEvaluator(argOut);
                    private final byte stt = ATypeTag.STRING.serialize();

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        argOut.reset();
                        evalStart.evaluate(tuple);
                        int start = IntegerSerializerDeserializer.getInt(argOut.getBytes(), 1) - 1;
                        argOut.reset();
                        evalLen.evaluate(tuple);
                        int len = IntegerSerializerDeserializer.getInt(argOut.getBytes(), 1);
                        argOut.reset();
                        evalString.evaluate(tuple);

                        byte[] bytes = argOut.getBytes();
                        int utflen = UTF8StringPointable.getUTFLen(bytes, 1);
                        int sStart = 3;
                        int c = 0;
                        int idxPos1 = 0;
                        // skip to start
                        while (idxPos1 < start && c < utflen) {
                            c += UTF8StringPointable.charSize(bytes, sStart + c);
                            ++idxPos1;
                        }
                        int startSubstr = c;
                        int idxPos2 = 0;
                        while (idxPos2 < len && c < utflen) {
                            c += UTF8StringPointable.charSize(bytes, sStart + c);
                            ++idxPos2;
                        }

                        if (idxPos2 < len) {
                            throw new AlgebricksException("substring: start=" + start + "\tlen=" + len
                                    + "\tgoing past the input length=" + (idxPos1 + idxPos2) + ".");
                        }

                        int substrByteLen = c - startSubstr;
                        try {
                            out.writeByte(stt);
                            out.writeByte((byte) ((substrByteLen >>> 8) & 0xFF));
                            out.writeByte((byte) ((substrByteLen >>> 0) & 0xFF));
                            out.write(bytes, sStart + startSubstr, substrByteLen);

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
        return FID;
    }

}
