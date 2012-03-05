package edu.uci.ics.asterix.runtime.evaluators.constructors;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.common.functions.FunctionConstants;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.AMutableTime;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.base.ATime;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IEvaluator;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IDataOutputProvider;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class ATimeConstructorDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public final static FunctionIdentifier FID = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "time", 1, false);
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
                    private short hour, minute, second, msecond, timezoneHour, timezoneMinute, value;
                    private byte timezonePart = 0;
                    private String errorMessage = "This can not be an instance of time";
                    private AMutableTime aTime = new AMutableTime(0, 0, 0, 0, 0, 0);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ATime> timeSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ATIME);
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
                                if (serString[offset + 2] != ':' || serString[offset + 5] != ':')
                                    throw new AlgebricksException(errorMessage);

                                hour = getValue(serString, offset, 2);
                                minute = getValue(serString, offset + 3, 2);
                                second = getValue(serString, offset + 6, 2);
                                msecond = 0;

                                if (serString[offset + 8] == ':') {
                                    msecond = getValue(serString, offset + 9, 3);
                                    if (hour < 0 || hour > 24 || minute < 0 || minute > 59 || second < 0 || second > 59
                                            || msecond < 0 || msecond > 999
                                            || (hour == 24 && (minute != 0 || second != 0 || msecond != 0)))
                                        throw new AlgebricksException(errorMessage);
                                    offset += 12;
                                } else {
                                    if (hour < 0 || hour > 24 || minute < 0 || minute > 59 || second < 0 || second > 59
                                            || (hour == 24 && (minute != 0 || second != 0)))
                                        throw new AlgebricksException(errorMessage);
                                    offset += 8;
                                }

                                if (outInput.getLength() > offset) {
                                    if (serString[offset] == 'Z')
                                        timezonePart = 0;
                                    else {
                                        if ((serString[offset] != '+' && serString[offset] != '-')
                                                || (serString[offset + 3] != ':'))
                                            throw new AlgebricksException(errorMessage);

                                        timezoneHour = getValue(serString, offset + 1, 2);
                                        timezoneMinute = getValue(serString, offset + 4, 2);

                                        if (timezoneHour < 0
                                                || timezoneHour > 24
                                                || (timezoneHour == 24 && timezoneMinute != 0)
                                                || (timezoneMinute != 0 && timezoneMinute != 15 && timezoneMinute != 30 && timezoneMinute != 45))
                                            throw new AlgebricksException(errorMessage);

                                        if (serString[offset] == '-')
                                            timezonePart = (byte) -((timezoneHour * 4) + timezoneMinute / 15);
                                        else
                                            timezonePart = (byte) ((timezoneHour * 4) + timezoneMinute / 15);
                                    }
                                }

                                aTime.setValue(hour, minute, second, msecond, 0, timezonePart);
                                timeSerde.serialize(aTime, out);

                            } else if (serString[0] == SER_NULL_TYPE_TAG)
                                nullSerde.serialize(ANull.NULL, out);
                            else
                                throw new AlgebricksException(errorMessage);
                        } catch (IOException e1) {
                            throw new AlgebricksException(errorMessage);
                        }
                    }

                    private short getValue(byte[] b, int offset, int numberOfDigits) throws AlgebricksException {
                        value = 0;
                        for (int i = 0; i < numberOfDigits; i++) {
                            if ((b[offset] >= '0' && b[offset] <= '9'))
                                value = (short) (value * 10 + b[offset++] - '0');
                            else
                                throw new AlgebricksException(errorMessage);
                        }
                        return value;
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