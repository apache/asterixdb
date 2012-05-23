package edu.uci.ics.asterix.runtime.evaluators.constructors;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.common.functions.FunctionConstants;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.AMutableTime;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.base.ATime;
import edu.uci.ics.asterix.om.base.temporal.GregorianCalendarSystem;
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

public class ATimeConstructorDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public final static FunctionIdentifier FID = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "time", 1, false);
    private final static byte SER_STRING_TYPE_TAG = ATypeTag.STRING.serialize();
    private final static byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new ATimeConstructorDescriptor();
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
                    private int offset;
                    private String errorMessage = "This can not be an instance of time";
                    private AMutableTime aTime = new AMutableTime(0);
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

                                int length = ((serString[1] & 0xff) << 8) + ((serString[2] & 0xff) << 0) + 3;

                                int hour = 0, min = 0, sec = 0, millis = 0;
                                int timezone = 0;
                                boolean isExtendedForm = false;
                                if (serString[offset + 2] == ':') {
                                    isExtendedForm = true;
                                }
                                if (isExtendedForm) {
                                    // parse extended form
                                    if (serString[offset] == '-' || serString[offset + 5] != ':')
                                        throw new AlgebricksException(errorMessage);

                                    for (int i = 0; i < 2; i++) {
                                        if ((serString[offset + i] >= '0' && serString[offset + i] <= '9'))
                                            hour = hour * 10 + serString[offset + i] - '0';
                                        else
                                            throw new AlgebricksException(errorMessage);

                                    }

                                    if (hour < 0 || hour > 23) {
                                        throw new AlgebricksException(errorMessage + ": hour " + hour);
                                    }

                                    offset += 3;

                                    for (int i = 0; i < 2; i++) {
                                        if ((serString[offset + i] >= '0' && serString[offset + i] <= '9'))
                                            min = min * 10 + serString[offset + i] - '0';
                                        else
                                            throw new AlgebricksException(errorMessage);

                                    }

                                    if (min < 0 || min > 59) {
                                        throw new AlgebricksException(errorMessage + ": min " + min);
                                    }

                                    offset += 3;

                                    for (int i = 0; i < 2; i++) {
                                        if ((serString[offset + i] >= '0' && serString[offset + i] <= '9'))
                                            sec = sec * 10 + serString[offset + i] - '0';
                                        else
                                            throw new AlgebricksException(errorMessage);

                                    }

                                    if (sec < 0 || sec > 59) {
                                        throw new AlgebricksException(errorMessage + ": sec " + sec);
                                    }

                                    offset += 2;

                                    if (length > offset && serString[offset] == '.') {

                                        offset++;
                                        int i = 0;
                                        for (; i < 3 && offset + i < length; i++) {
                                            if (serString[offset + i] >= '0' && serString[offset + i] <= '9') {
                                                millis = millis * 10 + serString[offset + i] - '0';
                                            } else {
                                                break;
                                            }
                                        }

                                        offset += i;

                                        for (; i < 3; i++) {
                                            millis = millis * 10;
                                        }

                                        for (; offset < length; offset++) {
                                            if (serString[offset] < '0' || serString[offset] > '9') {
                                                break;
                                            }
                                        }
                                    }

                                    if (length > offset) {
                                        if (serString[offset] != 'Z') {
                                            if ((serString[offset] != '+' && serString[offset] != '-')
                                                    || (serString[offset + 3] != ':'))
                                                throw new AlgebricksException(errorMessage);

                                            short timezoneHour = 0;
                                            short timezoneMinute = 0;

                                            for (int i = 0; i < 2; i++) {
                                                if ((serString[offset + 1 + i] >= '0' && serString[offset + 1 + i] <= '9'))
                                                    timezoneHour = (short) (timezoneHour * 10
                                                            + serString[offset + 1 + i] - '0');
                                                else
                                                    throw new AlgebricksException(errorMessage);

                                            }

                                            if (timezoneHour < GregorianCalendarSystem.TIMEZONE_HOUR_MIN
                                                    || timezoneHour > GregorianCalendarSystem.TIMEZONE_HOUR_MAX) {
                                                throw new AlgebricksException(errorMessage + ": time zone hour "
                                                        + timezoneHour);
                                            }

                                            for (int i = 0; i < 2; i++) {
                                                if ((serString[offset + 4 + i] >= '0' && serString[offset + 4 + i] <= '9'))
                                                    timezoneMinute = (short) (timezoneMinute * 10
                                                            + serString[offset + 4 + i] - '0');
                                                else
                                                    throw new AlgebricksException(errorMessage);
                                            }

                                            if (timezoneMinute < GregorianCalendarSystem.TIMEZONE_MIN_MIN
                                                    || timezoneMinute > GregorianCalendarSystem.TIMEZONE_MIN_MAX) {
                                                throw new AlgebricksException(errorMessage + ": time zone minute "
                                                        + timezoneMinute);
                                            }

                                            if (serString[offset] == '-')
                                                timezone = (byte) -((timezoneHour * 4) + timezoneMinute / 15);
                                            else
                                                timezone = (byte) ((timezoneHour * 4) + timezoneMinute / 15);
                                        }
                                    }

                                } else {
                                    // parse basic form

                                    if (serString[offset] == '-')
                                        throw new AlgebricksException(errorMessage);

                                    for (int i = 0; i < 2; i++) {
                                        if ((serString[offset + i] >= '0' && serString[offset + i] <= '9'))
                                            hour = hour * 10 + serString[offset + i] - '0';
                                        else
                                            throw new AlgebricksException(errorMessage);

                                    }

                                    if (hour < 0 || hour > 23) {
                                        throw new AlgebricksException(errorMessage + ": hour " + hour);
                                    }

                                    offset += 2;

                                    for (int i = 0; i < 2; i++) {
                                        if ((serString[offset + i] >= '0' && serString[offset + i] <= '9'))
                                            min = min * 10 + serString[offset + i] - '0';
                                        else
                                            throw new AlgebricksException(errorMessage);

                                    }

                                    if (min < 0 || min > 59) {
                                        throw new AlgebricksException(errorMessage + ": min " + min);
                                    }

                                    offset += 2;

                                    for (int i = 0; i < 2; i++) {
                                        if ((serString[offset + i] >= '0' && serString[offset + i] <= '9'))
                                            sec = sec * 10 + serString[offset + i] - '0';
                                        else
                                            throw new AlgebricksException(errorMessage);

                                    }

                                    if (sec < 0 || sec > 59) {
                                        throw new AlgebricksException(errorMessage + ": sec " + sec);
                                    }

                                    offset += 2;

                                    if (length > offset) {
                                        int i = 0;
                                        for (; i < 3 && offset + i < length; i++) {
                                            if ((serString[offset + i] >= '0' && serString[offset + i] <= '9')) {
                                                millis = millis * 10 + serString[offset + i] - '0';
                                            } else {
                                                break;
                                            }
                                        }

                                        offset += i;

                                        for (; i < 3; i++) {
                                            millis = millis * 10;
                                        }

                                        for (; offset < length; offset++) {
                                            if (serString[offset] < '0' || serString[offset] > '9') {
                                                break;
                                            }
                                        }
                                    }

                                    if (length > offset) {
                                        if (serString[offset] != 'Z') {
                                            if ((serString[offset] != '+' && serString[offset] != '-'))
                                                throw new AlgebricksException(errorMessage);

                                            short timezoneHour = 0;
                                            short timezoneMinute = 0;

                                            for (int i = 0; i < 2; i++) {
                                                if ((serString[offset + 1 + i] >= '0' && serString[offset + 1 + i] <= '9'))
                                                    timezoneHour = (short) (timezoneHour * 10
                                                            + serString[offset + 1 + i] - '0');
                                                else
                                                    throw new AlgebricksException(errorMessage);

                                            }

                                            for (int i = 0; i < 2; i++) {
                                                if ((serString[offset + 3 + i] >= '0' && serString[offset + 3 + i] <= '9'))
                                                    timezoneMinute = (short) (timezoneMinute * 10
                                                            + serString[offset + 3 + i] - '0');
                                                else
                                                    throw new AlgebricksException(errorMessage);

                                            }

                                            if (serString[offset] == '-')
                                                timezone = (byte) -((timezoneHour * 4) + timezoneMinute / 15);
                                            else
                                                timezone = (byte) ((timezoneHour * 4) + timezoneMinute / 15);
                                        }
                                    }
                                }

                                aTime.setValue(GregorianCalendarSystem.getInstance().getChronon(hour, min, sec, millis,
                                        timezone));

                                timeSerde.serialize(aTime, out);

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