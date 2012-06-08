package edu.uci.ics.asterix.runtime.evaluators.constructors;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.common.functions.FunctionConstants;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.base.AMutableInt32;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.IEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IDataOutputProvider;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class AInt32ConstructorDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public final static FunctionIdentifier FID = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "int32", 1, false);
    private final static byte SER_STRING_TYPE_TAG = ATypeTag.STRING.serialize();
    private final static byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new AInt32ConstructorDescriptor();
        }
    };

    @Override
    public IEvaluatorFactory createEvaluatorFactory(final IEvaluatorFactory[] args) {
        return new IEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
                return new IEvaluator() {

                    private DataOutput out = output.getDataOutput();

                    private ArrayBackedValueStorage outInput = new ArrayBackedValueStorage();
                    private IEvaluator eval = args[0].createEvaluator(outInput);
                    private int value, offset;
                    private boolean positive;
                    private String errorMessage = "This can not be an instance of int32";
                    private AMutableInt32 aInt32 = new AMutableInt32(0);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<AInt32> int32Serde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.AINT32);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ANULL);

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {

                        try {
                            outInput.reset();
                            eval.evaluate(tuple);
                            byte[] serString = outInput.getByteArray();
                            if (serString[0] == SER_STRING_TYPE_TAG) {
                                offset = 3;
                                value = 0;
                                positive = true;
                                if (serString[offset] == '+')
                                    offset++;
                                else if (serString[offset] == '-') {
                                    offset++;
                                    positive = false;
                                }
                                for (; offset < outInput.getLength(); offset++) {
                                    if (serString[offset] >= '0' && serString[offset] <= '9')
                                        value = value * 10 + serString[offset] - '0';
                                    else if (serString[offset] == 'i' && serString[offset + 1] == '3'
                                            && serString[offset + 2] == '2' && offset + 3 == outInput.getLength())
                                        break;
                                    else
                                        throw new AlgebricksException(errorMessage);
                                }
                                if (value < 0)
                                    throw new AlgebricksException(errorMessage);
                                if (value > 0 && !positive)
                                    value *= -1;

                                aInt32.setValue(value);
                                int32Serde.serialize(aInt32, out);
                            } else if (serString[0] == SER_NULL_TYPE_TAG)
                                nullSerde.serialize(ANull.NULL, out);
                            else
                                throw new AlgebricksException(errorMessage);
                        } catch (IOException e1) {
                            throw new AlgebricksException(errorMessage);
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