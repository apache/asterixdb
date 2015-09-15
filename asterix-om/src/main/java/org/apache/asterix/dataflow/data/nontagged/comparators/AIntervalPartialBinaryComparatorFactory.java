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

import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AIntervalSerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;

public class AIntervalPartialBinaryComparatorFactory implements IBinaryComparatorFactory {

    private static final long serialVersionUID = 1L;

    public static final AIntervalPartialBinaryComparatorFactory INSTANCE = new AIntervalPartialBinaryComparatorFactory();

    private AIntervalPartialBinaryComparatorFactory() {

    }

    /* (non-Javadoc)
     * @see org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory#createBinaryComparator()
     */
    @Override
    public IBinaryComparator createBinaryComparator() {
        return new IBinaryComparator() {

            @Override
            public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
                int c = Double.compare(
                        AInt64SerializerDeserializer.getLong(b1,
                                s1 + AIntervalSerializerDeserializer.getIntervalStartOffset()),
                        AInt64SerializerDeserializer.getLong(b2,
                                s2 + AIntervalSerializerDeserializer.getIntervalStartOffset()));
                if (c == 0) {
                    c = Double.compare(
                            AInt64SerializerDeserializer.getLong(b1,
                                    s1 + AIntervalSerializerDeserializer.getIntervalEndOffset()),
                            AInt64SerializerDeserializer.getLong(b2,
                                    s2 + AIntervalSerializerDeserializer.getIntervalEndOffset()));
                    if (c == 0) {
                        c = Byte.compare(b1[s1 + 16], b2[s2 + 16]);
                    }
                }
                return c;
            }
        };
    }

}
