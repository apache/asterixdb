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

import org.apache.hadoop.io.WritableComparator;

import edu.uci.ics.hivesterix.serde.lazy.LazyUtils;
import edu.uci.ics.hivesterix.serde.lazy.LazyUtils.VInt;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;

public class HiveStringBinaryDescComparatorFactory implements IBinaryComparatorFactory {
    private static final long serialVersionUID = 1L;

    public static HiveStringBinaryDescComparatorFactory INSTANCE = new HiveStringBinaryDescComparatorFactory();

    private HiveStringBinaryDescComparatorFactory() {
    }

    @Override
    public IBinaryComparator createBinaryComparator() {
        return new IBinaryComparator() {
            private VInt leftLen = new VInt();
            private VInt rightLen = new VInt();

            @Override
            public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
                LazyUtils.readVInt(b1, s1, leftLen);
                LazyUtils.readVInt(b2, s2, rightLen);

                if (leftLen.value + leftLen.length != l1 || rightLen.value + rightLen.length != l2)
                    throw new IllegalStateException("parse string: length mismatch, expected "
                            + (leftLen.value + leftLen.length) + ", " + (rightLen.value + rightLen.length)
                            + " but get " + l1 + ", " + l2);

                return -WritableComparator.compareBytes(b1, s1 + leftLen.length, l1 - leftLen.length, b2, s2
                        + rightLen.length, l2 - rightLen.length);
            }
        };
    }
}
