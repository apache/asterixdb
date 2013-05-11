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

import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt8SerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;

public class APolygonPartialBinaryComparatorFactory implements IBinaryComparatorFactory {

    private static final long serialVersionUID = 1L;

    public final static APolygonPartialBinaryComparatorFactory INSTANCE = new APolygonPartialBinaryComparatorFactory();

    private APolygonPartialBinaryComparatorFactory() {

    }

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory#createBinaryComparator()
     */
    @Override
    public IBinaryComparator createBinaryComparator() {
        return new IBinaryComparator() {

            @Override
            public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
                short pointCount1 = AInt8SerializerDeserializer.getByte(b1, s1);
                int c = Short.compare(pointCount1, AInt8SerializerDeserializer.getByte(b2, s2));

                if (c == 0) {
                    int ci = 0;
                    for (int i = 0; i < pointCount1; i++) {
                        ci = Double.compare(DoubleSerializerDeserializer.getDouble(b1, s1 + 3 + i * 16),
                                DoubleSerializerDeserializer.getDouble(b2, s1 + 3 + i * 16));
                        if (ci == 0) {
                            ci = Double.compare(DoubleSerializerDeserializer.getDouble(b1, s1 + 11 + i * 16),
                                    DoubleSerializerDeserializer.getDouble(b2, s1 + 11 + i * 16));
                            if(ci == 0){
                                continue;
                            }
                        }
                        return ci;
                    }
                }

                return c;
            }
        };
    }

}
