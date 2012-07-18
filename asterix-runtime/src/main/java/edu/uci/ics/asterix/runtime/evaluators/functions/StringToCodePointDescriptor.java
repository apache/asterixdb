package edu.uci.ics.asterix.runtime.evaluators.functions;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.builders.IAOrderedListBuilder;
import edu.uci.ics.asterix.builders.OrderedListBuilder;
import edu.uci.ics.asterix.common.functions.FunctionConstants;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.base.AMutableInt32;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IDataOutputProvider;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

/**
 * @author Xiaoyu Ma
 */
public class StringToCodePointDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;

    private final static FunctionIdentifier FID = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "string-to-codepoint", 1);
    public static final IFunctionDescriptorFactory FACTORY1 = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new StringToCodePointDescriptor();
        }
    };
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new StringToCodePointDescriptor();
        }
    };
    private final static byte SER_STRING_TYPE_TAG = ATypeTag.STRING.serialize();
    private final static byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) {
        return new ICopyEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
                return new ICopyEvaluator() {
                    protected final DataOutput out = output.getDataOutput();;
                    protected final ArrayBackedValueStorage argOut = new ArrayBackedValueStorage();
                    protected final ICopyEvaluator stringEval = args[0].createEvaluator(argOut);
                    protected final AOrderedListType intListType = new AOrderedListType(BuiltinType.AINT32, null);

                    private IAOrderedListBuilder listBuilder = new OrderedListBuilder();
                    private ArrayBackedValueStorage inputVal = new ArrayBackedValueStorage();

                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ANULL);
                    @SuppressWarnings("unchecked")
                    private final ISerializerDeserializer<AInt32> int32Serde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.AINT32);
                    private final AMutableInt32 aInt32 = new AMutableInt32(0);

                    int UTF8ToCodePoint(byte[] b, int s) {
                        if (b[s] >> 7 == 0) {
                            // 1 byte
                            return b[s];
                        } else if ((b[s] & 0xe0) == 0xc0) { /*0xe0 = 0b1110000*/
                            // 2 bytes
                            return ((int) (b[s] & 0x1f)) << 6 | /*0x3f = 0b00111111*/
                            ((int) (b[s + 1] & 0x3f));
                        } else if ((b[s] & 0xf0) == 0xe0) {
                            // 3bytes
                            return ((int) (b[s] & 0xf)) << 12 | ((int) (b[s + 1] & 0x3f)) << 6
                                    | ((int) (b[s + 2] & 0x3f));
                        } else if ((b[s] & 0xf8) == 0xf0) {
                            // 4bytes
                            return ((int) (b[s] & 0x7)) << 18 | ((int) (b[s + 1] & 0x3f)) << 12
                                    | ((int) (b[s + 2] & 0x3f)) << 6 | ((int) (b[s + 3] & 0x3f));
                        } else if ((b[s] & 0xfc) == 0xf8) {
                            // 5bytes
                            return ((int) (b[s] & 0x3)) << 24 | ((int) (b[s + 1] & 0x3f)) << 18
                                    | ((int) (b[s + 2] & 0x3f)) << 12 | ((int) (b[s + 3] & 0x3f)) << 6
                                    | ((int) (b[s + 4] & 0x3f));
                        } else if ((b[s] & 0xfe) == 0xfc) {
                            // 6bytes
                            return ((int) (b[s] & 0x1)) << 30 | ((int) (b[s + 1] & 0x3f)) << 24
                                    | ((int) (b[s + 2] & 0x3f)) << 18 | ((int) (b[s + 3] & 0x3f)) << 12
                                    | ((int) (b[s + 4] & 0x3f)) << 6 | ((int) (b[s + 5] & 0x3f));
                        }
                        return 0;
                    }

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        try {
                            argOut.reset();
                            stringEval.evaluate(tuple);
                            byte[] serString = argOut.getByteArray();

                            if (serString[0] == SER_STRING_TYPE_TAG) {
                                byte[] bytes = argOut.getByteArray();
                                int len = UTF8StringPointable.getUTFLength(bytes, 1);

                                int pos = 3;
                                listBuilder.reset(intListType);
                                while (pos < len + 3) {
                                    int codePoint = UTF8ToCodePoint(bytes, pos);
                                    pos += UTF8StringPointable.charSize(bytes, pos);

                                    inputVal.reset();
                                    aInt32.setValue(codePoint);
                                    int32Serde.serialize(aInt32, inputVal.getDataOutput());
                                    listBuilder.addItem(inputVal);

                                }
                                listBuilder.write(out, true);
                            } else if (serString[0] == SER_NULL_TYPE_TAG)
                                nullSerde.serialize(ANull.NULL, out);
                            else
                                throw new AlgebricksException("Expects String Type.");
                        } catch (IOException e1) {
                            throw new AlgebricksException(e1.getMessage());
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
