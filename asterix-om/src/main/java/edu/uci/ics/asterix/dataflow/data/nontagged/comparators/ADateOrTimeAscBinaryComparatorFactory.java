/*
 * Copyright 2009-2011 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.dataflow.data.nontagged.comparators;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;

public class ADateOrTimeAscBinaryComparatorFactory implements IBinaryComparatorFactory {

    private static final long serialVersionUID = 1L;

    public static final ADateOrTimeAscBinaryComparatorFactory INSTANCE = new ADateOrTimeAscBinaryComparatorFactory();

    private ADateOrTimeAscBinaryComparatorFactory() {
    }

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory#createBinaryComparator()
     */
    @Override
    public IBinaryComparator createBinaryComparator() {
        return new IBinaryComparator() {

            @Override
            public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
                int chrononTime1 = getInt(b1, s1);
                int chrononTime2 = getInt(b2, s2);

                if (chrononTime1 > chrononTime2) {
                    return 1;
                } else if (chrononTime1 < chrononTime2) {
                    return -1;
                } else {
                    return 0;
                }
            }

            private int getInt(byte[] bytes, int start) {
                return ((bytes[start] & 0xff) << 24) + ((bytes[start + 1] & 0xff) << 16)
                        + ((bytes[start + 2] & 0xff) << 8) + ((bytes[start + 3] & 0xff) << 0);
            }
        };
    }
}
