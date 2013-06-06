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
import edu.uci.ics.hivesterix.serde.lazy.LazyUtils.VInt;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;

public class HiveIntegerBinaryDescComparatorFactory implements IBinaryComparatorFactory {
    private static final long serialVersionUID = 1L;

    public static final HiveIntegerBinaryDescComparatorFactory INSTANCE = new HiveIntegerBinaryDescComparatorFactory();

    private HiveIntegerBinaryDescComparatorFactory() {
    }

    @Override
    public IBinaryComparator createBinaryComparator() {
        return new IBinaryComparator() {
            private VInt left = new VInt();
            private VInt right = new VInt();

            @Override
            public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
                LazyUtils.readVInt(b1, s1, left);
                LazyUtils.readVInt(b2, s2, right);
                if (left.length != l1 || right.length != l2)
                    throw new IllegalArgumentException("length mismatch in int comparator function actual: "
                            + left.length + " expected " + l1);
                if (left.value > right.value)
                    return -1;
                else if (left.value == right.value)
                    return 0;
                else
                    return 1;
            }
        };
    }
}
