package edu.uci.ics.asterix.runtime.evaluators.constructors;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.common.functions.FunctionConstants;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ADate;
import edu.uci.ics.asterix.om.base.AMutableDate;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.base.temporal.GregorianCalendarSystem;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IDataOutputProvider;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class ADateConstructorDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public final static FunctionIdentifier FID = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "date", 1, false);
    private final static byte SER_STRING_TYPE_TAG = ATypeTag.STRING.serialize();
    private final static byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new ADateConstructorDescriptor();
        }
    };

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) {
        return new ICopyEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
                return new ICopyEvaluator() {

                    private DataOutput out = output.getDataOutput();

                    private ArrayBackedValueStorage outInput = new ArrayBackedValueStorage();
                    private ICopyEvaluator eval = args[0].createEvaluator(outInput);
                    private int offset;
                    private String errorMessage = "This can not be an instance of date";
                    private AMutableDate aDate = new AMutableDate(0);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ADate> dateSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ADATE);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ANULL);

                    private GregorianCalendarSystem gCalInstance = GregorianCalendarSystem.getInstance();

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {

                        try {
                            outInput.reset();
                            eval.evaluate(tuple);
                            byte[] serString = outInput.getByteArray();
                            if (serString[0] == SER_STRING_TYPE_TAG) {

                                int length = ((serString[1] & 0xff) << 8) + ((serString[2] & 0xff) << 0) + 3;

                                offset = 3;

                                int year = 0, month = 0, day = 0;
                                int timezone = 0;
                                boolean positive = true;
                                if (serString[offset + 4] == '-' || serString[offset + 5] == '-') {
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

                                            for (int i = 0; i < 2; i++) {
                                                if ((serString[offset + 4 + i] >= '0' && serString[offset + 4 + i] <= '9'))
                                                    timezoneMinute = (short) (timezoneMinute * 10
                                                            + serString[offset + 4 + i] - '0');
                                                else
                                                    throw new AlgebricksException(errorMessage);

                                                if (serString[offset] == '-')
                                                    timezone = (byte) -((timezoneHour * 4) + timezoneMinute / 15);
                                                else
                                                    timezone = (byte) ((timezoneHour * 4) + timezoneMinute / 15);
                                            }
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

                                    if (length > offset) {
                                        if (serString[offset] != 'Z') {
                                            if ((serString[offset] != '+' && serString[offset] != '-'))
                                                throw new AlgebricksException(errorMessage);

                                            short timezoneHour = 0;
                                            short timezoneMinute = 0;

                                            for (int i = 0; i < 2; i++) {
                                                if (serString[offset + 1 + i] >= '0'
                                                        && serString[offset + 1 + i] <= '9')
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
                                                if (serString[offset + 3 + i] >= '0'
                                                        && serString[offset + 3 + i] <= '9')
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

                                long chrononTimeInMs = gCalInstance.getChronon(year, month, day, 0, 0, 0, 0, timezone);
                                aDate.setValue((int) (chrononTimeInMs / GregorianCalendarSystem.CHRONON_OF_DAY));
                                if (chrononTimeInMs < 0
                                        && chrononTimeInMs % GregorianCalendarSystem.CHRONON_OF_DAY != 0)
                                    aDate.setValue((int) (chrononTimeInMs / GregorianCalendarSystem.CHRONON_OF_DAY) - 1);

                                dateSerde.serialize(aDate, out);
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