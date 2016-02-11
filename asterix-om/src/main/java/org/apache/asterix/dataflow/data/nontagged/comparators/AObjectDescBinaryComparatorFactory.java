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

import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class AObjectDescBinaryComparatorFactory implements IBinaryComparatorFactory {

    private static final long serialVersionUID = 1L;

    public static final IBinaryComparatorFactory INSTANCE = new AObjectDescBinaryComparatorFactory();

    private AObjectDescBinaryComparatorFactory() {
    }

    @Override
    public IBinaryComparator createBinaryComparator() {
        return new ABinaryComparator() {
            final IBinaryComparator ascComp = AObjectAscBinaryComparatorFactory.INSTANCE.createBinaryComparator();

            // INTERVAL
            // Interval asc and desc comparator factories are not the inverse of each other.
            // Thus, we need to specify the interval desc comparator factory for descending comparisons.
            final IBinaryComparator descIntervalComp = AIntervalDescPartialBinaryComparatorFactory.INSTANCE
                    .createBinaryComparator();

            @Override
            public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) throws HyracksDataException {
                ATypeTag tag1 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(b1[s1]);
                ATypeTag tag2 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(b2[s2]);
                if (tag1 == ATypeTag.INTERVAL && tag2 == ATypeTag.INTERVAL) {
                    return descIntervalComp.compare(b1, s1 + 1, l1 - 1, b2, s2 + 1, l2 - 1);
                }
                return -ascComp.compare(b1, s1, l1, b2, s2, l2);
            }
        };
    }

}
