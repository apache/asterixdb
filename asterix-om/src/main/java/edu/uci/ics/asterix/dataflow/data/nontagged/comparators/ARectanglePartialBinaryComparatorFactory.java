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
package edu.uci.ics.asterix.dataflow.data.nontagged.comparators;

import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;

public class ARectanglePartialBinaryComparatorFactory implements IBinaryComparatorFactory {
    private static final long serialVersionUID = 1L;

    public final static ARectanglePartialBinaryComparatorFactory INSTANCE = new ARectanglePartialBinaryComparatorFactory();

    private ARectanglePartialBinaryComparatorFactory() {
    }

    @Override
    public IBinaryComparator createBinaryComparator() {

        return new IBinaryComparator() {

            @Override
            public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
                int c1 = Double.compare(ADoubleSerializerDeserializer.getDouble(b1, s1),
                        ADoubleSerializerDeserializer.getDouble(b2, s2));
                if (c1 == 0) {
                    int c2 = Double.compare(ADoubleSerializerDeserializer.getDouble(b1, s1 + 8),
                            ADoubleSerializerDeserializer.getDouble(b2, s2 + 8));
                    if (c2 == 0) {
                        int c3 = Double.compare(ADoubleSerializerDeserializer.getDouble(b1, s1 + 16),
                                ADoubleSerializerDeserializer.getDouble(b2, s2 + 16));
                        if (c3 == 0) {
                            int c4 = Double.compare(ADoubleSerializerDeserializer.getDouble(b1, s1 + 24),
                                    ADoubleSerializerDeserializer.getDouble(b2, s2 + 24));
                            return c4;
                        } else {
                            return c3;
                        }
                    } else {
                        return c2;
                    }
                } else {
                    return c1;
                }
            }
        };
    }

}