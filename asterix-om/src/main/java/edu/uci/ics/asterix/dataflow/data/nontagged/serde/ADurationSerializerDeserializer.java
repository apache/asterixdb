package edu.uci.ics.asterix.dataflow.data.nontagged.serde;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ADuration;
import edu.uci.ics.asterix.om.base.AMutableDuration;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class ADurationSerializerDeserializer implements ISerializerDeserializer<ADuration> {

    private static final long serialVersionUID = 1L;

    public static final ADurationSerializerDeserializer INSTANCE = new ADurationSerializerDeserializer();

    private static AMutableDuration aDuration = new AMutableDuration(0, 0);
    @SuppressWarnings("unchecked")
    private static ISerializerDeserializer<ADuration> durationSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ADURATION);
    private static String errorMessage = " can not be an instance of duration";

    private ADurationSerializerDeserializer() {
    }

    @Override
    public ADuration deserialize(DataInput in) throws HyracksDataException {
        try {
            return new ADuration(in.readInt(), in.readLong());
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void serialize(ADuration instance, DataOutput out) throws HyracksDataException {
        try {
            out.writeInt(instance.getMonths());
            out.writeLong(instance.getMilliseconds());
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    public static void parse(String duration, DataOutput out) throws HyracksDataException {
        try {
            boolean positive = true;
            int offset = 0;
            int value = 0, hour = 0, minute = 0, second = 0, millisecond = 0, year = 0, month = 0, day = 0;
            boolean isYear = true, isMonth = true, isDay = true, isHour = true, isMinute = true, isSecond = true, isMillisecond = true;
            boolean isTime = false;
            boolean timeItem = true;
            if (duration.charAt(offset) == '-') {
                offset++;
                positive = false;
            }

            if (duration.charAt(offset++) != 'P')
                throw new HyracksDataException(duration + errorMessage);

            for (; offset < duration.length(); offset++) {
                if (duration.charAt(offset) >= '0' && duration.charAt(offset) <= '9')
                    // accumulate the digit fields
                    value = value * 10 + duration.charAt(offset) - '0';
                else {
                    switch (duration.charAt(offset)) {
                        case 'Y':
                            if (isYear) {
                                year = value;
                                isYear = false;
                            } else
                                throw new HyracksDataException(duration + errorMessage);
                            break;
                        case 'M':
                            if (!isTime) {
                                if (isMonth) {
                                    month = value;
                                    isMonth = false;
                                } else
                                    throw new HyracksDataException(duration + errorMessage);
                            } else if (isMinute) {
                                minute = value;
                                isMinute = false;
                                timeItem = false;
                            } else
                                throw new HyracksDataException(duration + errorMessage);
                            break;
                        case 'D':
                            if (isDay) {
                                day = value;
                                isDay = false;
                            } else
                                throw new HyracksDataException(duration + errorMessage);
                            break;
                        case 'T':
                            if (!isTime) {
                                isTime = true;
                                timeItem = true;
                            } else
                                throw new HyracksDataException(duration + errorMessage);
                            break;

                        case 'H':
                            if (isHour) {
                                hour = value;
                                isHour = false;
                                timeItem = false;
                            } else
                                throw new HyracksDataException(duration + errorMessage);
                            break;
                        case 'S':
                            if (isSecond) {
                                second = value;
                                isSecond = false;
                                timeItem = false;
                            } else
                                throw new HyracksDataException(duration + errorMessage);
                            break;
                        case '.':
                            if (isMillisecond) {
                                millisecond = value;
                                isMillisecond = false;
                                timeItem = false;
                            } else
                                throw new HyracksDataException(duration + errorMessage);
                            break;
                        default:
                            throw new HyracksDataException(duration + errorMessage);

                    }
                    value = 0;
                }
            }

            if (isTime && timeItem)
                throw new HyracksDataException(duration + errorMessage);

            //            if (isYear && isMonth && isDay && !isTime)
            //                throw new HyracksDataException(duration + errorMessage);

            if (positive)
                aDuration.setValue(year * 12 + month, day * 24 * 3600 * 1000L + 3600 * 1000L * hour + 60 * minute
                        * 1000L + second * 1000L + millisecond);
            else
                aDuration
                        .setValue(-1 * (year * 12 + month),
                                -1
                                        * (day * 24 * 3600 * 1000L + 3600 * 1000L * hour + 60 * minute * 1000L + second
                                                * 1000L + millisecond));
            durationSerde.serialize(aDuration, out);
        } catch (HyracksDataException e) {
            throw new HyracksDataException(duration + errorMessage);
        }
    }
}
