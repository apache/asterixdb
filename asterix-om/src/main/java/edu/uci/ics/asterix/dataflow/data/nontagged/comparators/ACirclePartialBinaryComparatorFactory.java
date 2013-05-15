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

import edu.uci.ics.asterix.dataflow.data.nontagged.Coordinate;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ACircleSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class ACirclePartialBinaryComparatorFactory implements IBinaryComparatorFactory {

    private static final long serialVersionUID = 1L;

    public static final ACirclePartialBinaryComparatorFactory INSTANCE = new ACirclePartialBinaryComparatorFactory();

    private ACirclePartialBinaryComparatorFactory() {

    }

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory#createBinaryComparator()
     */
    @Override
    public IBinaryComparator createBinaryComparator() {
        return new IBinaryComparator() {

            @Override
            public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
                try {
                    // center.x
                    int c = Double
                            .compare(
                                    ADoubleSerializerDeserializer.getDouble(
                                            b1,
                                            s1
                                                    + ACircleSerializerDeserializer
                                                            .getCenterPointCoordinateOffset(Coordinate.X) - 1),
                                    ADoubleSerializerDeserializer.getDouble(
                                            b2,
                                            s2
                                                    + ACircleSerializerDeserializer
                                                            .getCenterPointCoordinateOffset(Coordinate.X) - 1));
                    if (c == 0) {
                        // center.y
                        c = Double.compare(
                                ADoubleSerializerDeserializer.getDouble(b1,
                                        s1 + ACircleSerializerDeserializer.getCenterPointCoordinateOffset(Coordinate.Y)
                                                - 1),
                                ADoubleSerializerDeserializer.getDouble(b2,
                                        s2 + ACircleSerializerDeserializer.getCenterPointCoordinateOffset(Coordinate.Y)
                                                - 1));
                        if (c == 0) {
                            // radius
                            return Double.compare(
                                    ADoubleSerializerDeserializer.getDouble(b1,
                                            s1 + ACircleSerializerDeserializer.getRadiusOffset() - 1),
                                    ADoubleSerializerDeserializer.getDouble(b2,
                                            s2 + ACircleSerializerDeserializer.getRadiusOffset() - 1));
                        }
                    }
                    return c;
                } catch (HyracksDataException hex) {
                    throw new IllegalStateException(hex);
                }
            }
        };
    }
}
