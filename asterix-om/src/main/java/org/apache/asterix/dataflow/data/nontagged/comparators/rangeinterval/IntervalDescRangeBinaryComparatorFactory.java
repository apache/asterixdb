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
package org.apache.asterix.dataflow.data.nontagged.comparators.rangeinterval;

import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AIntervalSerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryRangeComparatorFactory;

public class IntervalDescRangeBinaryComparatorFactory implements IBinaryRangeComparatorFactory {

    private static final long serialVersionUID = 1L;

    public static final IBinaryRangeComparatorFactory INSTANCE = new IntervalDescRangeBinaryComparatorFactory();

    private IntervalDescRangeBinaryComparatorFactory() {

    }

    @Override
    public IBinaryComparator createMinBinaryComparator() {
        return new IBinaryComparator() {

            @Override
            public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
                return -Long.compare(AIntervalSerializerDeserializer.getIntervalEnd(b1, s1),
                        AInt64SerializerDeserializer.getLong(b2, s2));
            }
        };
    }

    @Override
    public IBinaryComparator createMaxBinaryComparator() {
        return new IBinaryComparator() {

            @Override
            public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
                return -Long.compare(AIntervalSerializerDeserializer.getIntervalStart(b1, s1),
                        AInt64SerializerDeserializer.getLong(b2, s2));
            }
        };
    }
}
