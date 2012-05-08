package edu.uci.ics.asterix.runtime.evaluators.constructors;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.common.functions.FunctionConstants;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ADuration;
import edu.uci.ics.asterix.om.base.AMutableDuration;
import edu.uci.ics.asterix.om.base.ANull;
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

public class ADurationConstructorDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public final static FunctionIdentifier FID = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "duration", 1,
            false);
    private final static byte SER_STRING_TYPE_TAG = ATypeTag.STRING.serialize();
    private final static byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();

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
                    private int offset;
                    private int value = 0, hour = 0, minute = 0, second = 0, year = 0, month = 0, day = 0;
                    private boolean isYear = true, isMonth = true, isDay = true, isHour = true, isMinute = true,
                            isSecond = true, isTime = false, timeItem = true, positive = true;
                    private String errorMessage = "This can not be an instance of duration";
                    private AMutableDuration aDuration = new AMutableDuration(0, 0);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ADuration> durationSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ADURATION);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ANULL);

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {

                        try {
                            outInput.reset();
                            eval.evaluate(tuple);
                            byte[] serString = outInput.getBytes();
                            if (serString[0] == SER_STRING_TYPE_TAG) {
                                offset = 3;
                                if (serString[offset] == '-') {
                                    offset++;
                                    positive = false;
                                }
                                if (serString[offset++] != 'D')
                                    throw new AlgebricksException(errorMessage);

                                for (; offset < outInput.getLength(); offset++) {
                                    if (serString[offset] >= '0' && serString[offset] <= '9')
                                        value = value * 10 + serString[offset] - '0';
                                    else {
                                        switch (serString[offset]) {
                                            case 'Y':
                                                if (isYear) {
                                                    year = value;
                                                    isYear = false;
                                                } else
                                                    throw new AlgebricksException(errorMessage);
                                                break;
                                            case 'M':
                                                if (!isTime) {
                                                    if (isMonth) {
                                                        if (value < 0 || value > 11)
                                                            throw new AlgebricksException(errorMessage);
                                                        else {
                                                            month = value;
                                                            isMonth = false;
                                                        }
                                                    } else
                                                        throw new AlgebricksException(errorMessage);
                                                } else if (isMinute) {
                                                    if (value < 0 || value > 59)
                                                        throw new AlgebricksException(errorMessage);
                                                    else {
                                                        minute = value;
                                                        isMinute = false;
                                                        timeItem = false;
                                                    }
                                                } else
                                                    throw new AlgebricksException(errorMessage);
                                                break;
                                            case 'D':
                                                if (isDay) {
                                                    if (value < 0 || value > 30)
                                                        throw new AlgebricksException(errorMessage);
                                                    else {
                                                        day = value;
                                                        isDay = false;
                                                    }
                                                } else
                                                    throw new AlgebricksException(errorMessage);
                                                break;
                                            case 'T':
                                                if (!isTime) {
                                                    isTime = true;
                                                    timeItem = true;
                                                } else
                                                    throw new AlgebricksException(errorMessage);
                                                break;

                                            case 'H':
                                                if (isHour) {
                                                    if (value < 0 || value > 23)
                                                        throw new AlgebricksException(errorMessage);
                                                    else {
                                                        hour = value;
                                                        isHour = false;
                                                        timeItem = false;
                                                    }
                                                } else
                                                    throw new AlgebricksException(errorMessage);
                                                break;
                                            case 'S':
                                                if (isSecond) {
                                                    if (value < 0 || value > 59)
                                                        throw new AlgebricksException(errorMessage);
                                                    else {
                                                        second = value;
                                                        isSecond = false;
                                                        timeItem = false;
                                                    }
                                                } else
                                                    throw new AlgebricksException(errorMessage);
                                                break;
                                            default:
                                                throw new AlgebricksException(errorMessage);

                                        }
                                        value = 0;
                                    }
                                }

                                if (isTime && timeItem)
                                    throw new AlgebricksException(errorMessage);

                                if (isYear && isMonth && isDay && !isTime)
                                    throw new AlgebricksException(errorMessage);

                                if (positive)
                                    aDuration.setValue(year * 12 + month, day * 24 * 3600 + 3600 * hour + 60 * minute
                                            + second);
                                else
                                    aDuration.setValue(-1 * (year * 12 + month), -1
                                            * (day * 24 * 3600 + 3600 * hour + 60 * minute + second));
                                durationSerde.serialize(aDuration, out);
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