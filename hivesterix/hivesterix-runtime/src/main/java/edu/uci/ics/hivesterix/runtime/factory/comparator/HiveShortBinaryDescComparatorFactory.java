/*
 * Copyright 2009-2013 by The Regents of the University of California
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
package edu.uci.ics.hivesterix.runtime.factory.comparator;

import edu.uci.ics.hivesterix.serde.lazy.LazyUtils;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;

public class HiveShortBinaryDescComparatorFactory implements IBinaryComparatorFactory {
    private static final long serialVersionUID = 1L;

    public static HiveShortBinaryDescComparatorFactory INSTANCE = new HiveShortBinaryDescComparatorFactory();

    private HiveShortBinaryDescComparatorFactory() {
    }

    @Override
    public IBinaryComparator createBinaryComparator() {
        return new IBinaryComparator() {
            private short left;
            private short right;

            @Override
            public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
                left = LazyUtils.byteArrayToShort(b1, s1);
                right = LazyUtils.byteArrayToShort(b2, s2);
                if (left > right)
                    return -1;
                else if (left == right)
                    return 0;
                else
                    return 1;
            }
        };
    }

}
