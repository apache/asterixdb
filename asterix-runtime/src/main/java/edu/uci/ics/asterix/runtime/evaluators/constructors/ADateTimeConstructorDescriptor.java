package edu.uci.ics.asterix.runtime.evaluators.constructors;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.common.functions.FunctionConstants;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ADateTime;
import edu.uci.ics.asterix.om.base.AMutableDateTime;
import edu.uci.ics.asterix.om.base.ANull;
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

public class ADateTimeConstructorDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public final static FunctionIdentifier FID = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "datetime", 1,
            false);
    private final static byte SER_STRING_TYPE_TAG = ATypeTag.STRING.serialize();
    private final static byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new ADateTimeConstructorDescriptor();
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
                    private String errorMessage = "This can not be an instance of datetime";
                    private AMutableDateTime aDateTime = new AMutableDateTime(0L);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ADateTime> datetimeSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ADATETIME);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ANULL);

                    private GregorianCalendarSystem gCalInstance = GregorianCalendarSystem.getInstance();

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {

                        try {
                            outInput.reset();
                            eval.evaluate(tuple);
                            byte[] serString = outInput.getBytes();
                            if (serString[0] == SER_STRING_TYPE_TAG) {
                                int length = ((serString[1] & 0xff) << 8) + ((serString[2] & 0xff) << 0) + 3;
                                offset = 3;

                                int year = 0, month = 0, day = 0, hour = 0, min = 0, sec = 0, millis = 0;
                                int timezone = 0;

                                boolean positive = true;
                                boolean isExtendedForm = false;
                                if (serString[offset + 13] == ':' || serString[offset + 14] == ':') {
                                    isExtendedForm = true;
                                }
                                if (isExtendedForm) {
                                    // parse extended form
                                    if (serString[offset] == '-') {
                                        offset++;
                                        positive = false;
                                    }

                                    if (serString[offset + 4] != '-' || serString[offset + 7] != '-')
                                        throw new AlgebricksException(errorMessage);

                                    // year
                                    for (int i = 0; i < 4; i++) {
                                        if ((serString[offset + i] >= '0' && serString[offset + i] <= '9')) {
                                            year = year * 10 + serString[offset + i] - '0';
                                        } else {
                                            throw new AlgebricksException(errorMessage);
                                        }
                                    }

                                    if (year < GregorianCalendarSystem.FIELD_MINS[GregorianCalendarSystem.YEAR]
                                            || year > GregorianCalendarSystem.FIELD_MAXS[GregorianCalendarSystem.YEAR]) {
                                        throw new AlgebricksException(errorMessage + ": year " + year);
                                    }

                                    offset += 5;

                                    // month
                                    for (int i = 0; i < 2; i++) {
                                        if ((serString[offset + i] >= '0' && serString[offset + i] <= '9')) {
                                            month = month * 10 + serString[offset + i] - '0';
                                        } else {
                                            throw new AlgebricksException(errorMessage);
                                        }
                                    }

                                    if (month < GregorianCalendarSystem.FIELD_MINS[GregorianCalendarSystem.MONTH]
                                            || month > GregorianCalendarSystem.FIELD_MAXS[GregorianCalendarSystem.MONTH]) {
                                        throw new AlgebricksException(errorMessage + ": month " + month);
                                    }

                                    offset += 3;

                                    // day
                                    for (int i = 0; i < 2; i++) {
                                        if ((serString[offset + i] >= '0' && serString[offset + i] <= '9')) {
                                            day = day * 10 + serString[offset + i] - '0';
                                        } else {
                                            throw new AlgebricksException(errorMessage);
                                        }
                                    }

                                    if (day < GregorianCalendarSystem.FIELD_MINS[GregorianCalendarSystem.DAY]
                                            || day > GregorianCalendarSystem.FIELD_MAXS[GregorianCalendarSystem.DAY]) {
                                        throw new AlgebricksException(errorMessage + ": day " + day);
                                    }

                                    offset += 2;

                                    if (!positive)
                                        year *= -1;

                                    // skip the "T" separator
                                    offset += 1;

                                    if (serString[offset + 2] != ':' || serString[offset + 5] != ':')
                                        throw new AlgebricksException(errorMessage);

                                    // hour
                                    for (int i = 0; i < 2; i++) {
                                        if ((serString[offset + i] >= '0' && serString[offset + i] <= '9')) {
                                            hour = hour * 10 + serString[offset + i] - '0';
                                        } else {
                                            throw new AlgebricksException(errorMessage);
                                        }
                                    }

                                    if (hour < GregorianCalendarSystem.FIELD_MINS[GregorianCalendarSystem.HOUR]
                                            || hour > GregorianCalendarSystem.FIELD_MAXS[GregorianCalendarSystem.HOUR]) {
                                        throw new AlgebricksException(errorMessage + ": hour " + hour);
                                    }

                                    offset += 3;

                                    // minute
                                    for (int i = 0; i < 2; i++) {
                                        if ((serString[offset + i] >= '0' && serString[offset + i] <= '9')) {
                                            min = min * 10 + serString[offset + i] - '0';
                                        } else {
                                            throw new AlgebricksException(errorMessage);
                                        }
                                    }

                                    if (min < GregorianCalendarSystem.FIELD_MINS[GregorianCalendarSystem.MINUTE]
                                            || min > GregorianCalendarSystem.FIELD_MAXS[GregorianCalendarSystem.MINUTE]) {
                                        throw new AlgebricksException(errorMessage + ": min " + min);
                                    }

                                    offset += 3;

                                    // second
                                    for (int i = 0; i < 2; i++) {
                                        if ((serString[offset + i] >= '0' && serString[offset + i] <= '9')) {
                                            sec = sec * 10 + serString[offset + i] - '0';
                                        } else {
                                            throw new AlgebricksException(errorMessage);
                                        }
                                    }

                                    if (sec < GregorianCalendarSystem.FIELD_MINS[GregorianCalendarSystem.SECOND]
                                            || sec > GregorianCalendarSystem.FIELD_MAXS[GregorianCalendarSystem.SECOND]) {
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
                                    if (serString[offset] == '-') {
                                        offset++;
                                        positive = false;
                                    }

                                    // year
                                    for (int i = 0; i < 4; i++) {
                                        if ((serString[offset + i] >= '0' && serString[offset + i] <= '9')) {
                                            year = year * 10 + serString[offset + i] - '0';
                                        } else {
                                            throw new AlgebricksException(errorMessage);
                                        }
                                    }

                                    if (year < GregorianCalendarSystem.FIELD_MINS[GregorianCalendarSystem.YEAR]
                                            || year > GregorianCalendarSystem.FIELD_MAXS[GregorianCalendarSystem.YEAR]) {
                                        throw new AlgebricksException(errorMessage + ": year " + year);
                                    }

                                    offset += 4;

                                    // month
                                    for (int i = 0; i < 2; i++) {
                                        if ((serString[offset + i] >= '0' && serString[offset + i] <= '9')) {
                                            month = month * 10 + serString[offset + i] - '0';
                                        } else {
                                            throw new AlgebricksException(errorMessage);
                                        }
                                    }

                                    if (month < GregorianCalendarSystem.FIELD_MINS[GregorianCalendarSystem.MONTH]
                                            || month > GregorianCalendarSystem.FIELD_MAXS[GregorianCalendarSystem.MONTH]) {
                                        throw new AlgebricksException(errorMessage + ": month " + month);
                                    }

                                    offset += 2;

                                    // day
                                    for (int i = 0; i < 2; i++) {
                                        if ((serString[offset + i] >= '0' && serString[offset + i] <= '9')) {
                                            day = day * 10 + serString[offset + i] - '0';
                                        } else {
                                            throw new AlgebricksException(errorMessage);
                                        }
                                    }

                                    if (day < GregorianCalendarSystem.FIELD_MINS[GregorianCalendarSystem.DAY]
                                            || day > GregorianCalendarSystem.FIELD_MAXS[GregorianCalendarSystem.DAY]) {
                                        throw new AlgebricksException(errorMessage + ": day " + day);
                                    }

                                    offset += 2;

                                    if (!positive)
                                        year *= -1;

                                    // hour
                                    for (int i = 0; i < 2; i++) {
                                        if ((serString[offset + i] >= '0' && serString[offset + i] <= '9')) {
                                            hour = hour * 10 + serString[offset + i] - '0';
                                        } else {
                                            throw new AlgebricksException(errorMessage);
                                        }
                                    }

                                    if (hour < GregorianCalendarSystem.FIELD_MINS[GregorianCalendarSystem.HOUR]
                                            || hour > GregorianCalendarSystem.FIELD_MAXS[GregorianCalendarSystem.HOUR]) {
                                        throw new AlgebricksException(errorMessage + ": hour " + hour);
                                    }

                                    offset += 2;

                                    // minute
                                    for (int i = 0; i < 2; i++) {
                                        if ((serString[offset + i] >= '0' && serString[offset + i] <= '9')) {
                                            min = min * 10 + serString[offset + i] - '0';
                                        } else {
                                            throw new AlgebricksException(errorMessage);
                                        }
                                    }

                                    if (min < GregorianCalendarSystem.FIELD_MINS[GregorianCalendarSystem.MINUTE]
                                            || min > GregorianCalendarSystem.FIELD_MAXS[GregorianCalendarSystem.MINUTE]) {
                                        throw new AlgebricksException(errorMessage + ": min " + min);
                                    }

                                    offset += 2;

                                    // second
                                    for (int i = 0; i < 2; i++) {
                                        if ((serString[offset + i] >= '0' && serString[offset + i] <= '9')) {
                                            sec = sec * 10 + serString[offset + i] - '0';
                                        } else {
                                            throw new AlgebricksException(errorMessage);
                                        }
                                    }

                                    if (sec < GregorianCalendarSystem.FIELD_MINS[GregorianCalendarSystem.SECOND]
                                            || sec > GregorianCalendarSystem.FIELD_MAXS[GregorianCalendarSystem.SECOND]) {
                                        throw new AlgebricksException(errorMessage + ": sec " + sec);
                                    }

                                    offset += 2;

                                    if (length > offset) {
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

                                            if (timezoneHour < GregorianCalendarSystem.TIMEZONE_HOUR_MIN
                                                    || timezoneHour > GregorianCalendarSystem.TIMEZONE_HOUR_MAX) {
                                                throw new AlgebricksException(errorMessage + ": time zone hour "
                                                        + timezoneHour);
                                            }

                                            for (int i = 0; i < 2; i++) {
                                                if ((serString[offset + 3 + i] >= '0' && serString[offset + 3 + i] <= '9'))
                                                    timezoneMinute = (short) (timezoneMinute * 10
                                                            + serString[offset + 3 + i] - '0');
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
                                }

                                aDateTime.setValue(gCalInstance.getChronon(year, month, day, hour, min, sec,
                                        (int) millis, timezone));

                                datetimeSerde.serialize(aDateTime, out);
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