/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.dataflow.data.nontagged.comparators;

import org.apache.asterix.dataflow.data.nontagged.serde.AIntervalSerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;

public class AIntervalDescPartialBinaryComparatorFactory implements IBinaryComparatorFactory {

    private static final long serialVersionUID = 1L;

    public static final AIntervalDescPartialBinaryComparatorFactory INSTANCE = new AIntervalDescPartialBinaryComparatorFactory();

    private AIntervalDescPartialBinaryComparatorFactory() {

    }

    /* (non-Javadoc)
     * @see org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory#createBinaryComparator()
     */
    @Override
    public IBinaryComparator createBinaryComparator() {
        return new IBinaryComparator() {

            @Override
            public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
                // The descending interval comparator sorts intervals first by end point, then by start point.
                // If the interval have the same point values, the final comparison orders the intervals by type
                // (time, date, datetime).
                int c = Long.compare(AIntervalSerializerDeserializer.getIntervalEnd(b1, s1),
                        AIntervalSerializerDeserializer.getIntervalEnd(b2, s2));
                if (c == 0) {
                    c = Long.compare(AIntervalSerializerDeserializer.getIntervalStart(b1, s1),
                            AIntervalSerializerDeserializer.getIntervalStart(b2, s2));
                    if (c == 0) {
                        c = Byte.compare(AIntervalSerializerDeserializer.getIntervalTimeType(b1, s1),
                                AIntervalSerializerDeserializer.getIntervalTimeType(b2, s2));
                    }
                }
                // Since the comparisons are based on ascending order, the result is reversed.
                return -c;
            }
        };
    }

}
