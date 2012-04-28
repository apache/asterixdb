package edu.uci.ics.asterix.dataflow.data.nontagged.comparators;

import edu.uci.ics.asterix.om.base.AMutableDateTime;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;

public class ADateTimeAscBinaryComparatorFactory implements IBinaryComparatorFactory {
    private static final long serialVersionUID = 1L;

    public static final ADateTimeAscBinaryComparatorFactory INSTANCE = new ADateTimeAscBinaryComparatorFactory();

    private ADateTimeAscBinaryComparatorFactory() {
    }

    @Override
    public IBinaryComparator createBinaryComparator() {
        return new IBinaryComparator() {
        	
            private AMutableDateTime dt1 = new AMutableDateTime(0, 0, 0, 0, 0, 0, 0, 0, 0);
            private AMutableDateTime dt2 = new AMutableDateTime(0, 0, 0, 0, 0, 0, 0, 0, 0);

            @Override
            public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
                short year = (short) (((b1[s1] & 0xff) << 8) | b1[s1 + 1] & 0xff);
                int time = ((b1[s1 + 4] & 0xff) << 24) | ((b1[s1 + 5] & 0xff) << 16) | ((b1[s1 + 6] & 0xff) << 8)
                        | (b1[s1 + 7] & 0xff);

                dt1.setValue(year >> 1, // year
                        (year & 0x0001) * 8 + ((b1[s1 + 2] >>> 5) & 0x07), // month
                        b1[s1 + 2] & 0x1f, // day
                        (short) ((time) * 20 % 216000000 / 3600000), // hour
                        (short) ((time) * 20 % 3600000 / 60000), // minutes
                        (short) ((time) * 20 % 60000 / 1000), // seconds
                        (short) ((time) * 20 % 1000), // milliseconds
                        0, // microseconds
                        b1[s1 + 3]); // timezone

                year = (short) (((b2[s2] & 0xff) << 8) | b2[s2 + 1] & 0xff);
                time = ((b2[s2 + 4] & 0xff) << 24) | ((b2[s2 + 5] & 0xff) << 16) | ((b2[s2 + 6] & 0xff) << 8)
                        | (b2[s2 + 7] & 0xff);

                dt2.setValue(year >> 1, // year
                        (year & 0x0001) * 8 + ((b2[s2 + 2] >>> 5) & 0x07), // month
                        b2[s2 + 2] & 0x1f, // day
                        (short) ((time) * 20 % 216000000 / 3600000), // hour
                        (short) ((time) * 20 % 3600000 / 60000), // minutes
                        (short) ((time) * 20 % 60000 / 1000), // seconds
                        (short) ((time) * 20 % 1000), // milliseconds
                        0, // microseconds
                        b2[s2 + 3]); // timezone

                return dt1.compare(dt2);
            }

        };
    }

}
