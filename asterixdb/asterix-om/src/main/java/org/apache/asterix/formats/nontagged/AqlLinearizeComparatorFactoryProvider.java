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
package org.apache.asterix.formats.nontagged;

import java.io.Serializable;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.data.ILinearizeComparatorFactoryProvider;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ILinearizeComparator;
import org.apache.hyracks.api.dataflow.value.ILinearizeComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.rtree.linearize.HilbertDoubleComparatorFactory;
import org.apache.hyracks.storage.am.rtree.linearize.ZCurveDoubleComparatorFactory;
import org.apache.hyracks.storage.am.rtree.linearize.ZCurveIntComparatorFactory;

public class AqlLinearizeComparatorFactoryProvider implements ILinearizeComparatorFactoryProvider, Serializable {

    private static final long serialVersionUID = 1L;
    public static final AqlLinearizeComparatorFactoryProvider INSTANCE = new AqlLinearizeComparatorFactoryProvider();

    private AqlLinearizeComparatorFactoryProvider() {
    }

    @Override
    public ILinearizeComparatorFactory getLinearizeComparatorFactory(Object type, boolean ascending, int dimension)
            throws AlgebricksException {
        ATypeTag typeTag = (ATypeTag) type;

        if (dimension == 2 && (typeTag == ATypeTag.DOUBLE)) {
            return addOffset(new HilbertDoubleComparatorFactory(2), ascending);
        } else if (typeTag == ATypeTag.DOUBLE) {
            return addOffset(new ZCurveDoubleComparatorFactory(dimension), ascending);
        } else if (typeTag == ATypeTag.INT8 || typeTag == ATypeTag.INT16 || typeTag == ATypeTag.INT32
                || typeTag == ATypeTag.INT64) {
            return addOffset(new ZCurveIntComparatorFactory(dimension), ascending);
        } else {
            throw new AlgebricksException("Cannot propose linearizer for key with type " + typeTag + " and dimension "
                    + dimension + ".");
        }
    }

    private ILinearizeComparatorFactory addOffset(final IBinaryComparatorFactory inst, final boolean ascending) {
        return new ILinearizeComparatorFactory() {

            private static final long serialVersionUID = 1L;

            @Override
            public ILinearizeComparator createBinaryComparator() {
                final ILinearizeComparator bc = (ILinearizeComparator) inst.createBinaryComparator();
                final int dimension = bc.getDimensions();
                if (ascending) {
                    return new ILinearizeComparator() {

                        @Override
                        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
                                throws HyracksDataException {
                            return bc.compare(b1, s1 + 1, l1, b2, s2 + 1, l2);
                        }

                        @Override
                        public int getDimensions() {
                            // TODO Auto-generated method stub
                            return dimension;
                        }
                    };
                } else {
                    return new ILinearizeComparator() {

                        @Override
                        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
                                throws HyracksDataException {
                            return -bc.compare(b1, s1 + 1, l1, b2, s2 + 1, l2);
                        }

                        @Override
                        public int getDimensions() {
                            // TODO Auto-generated method stub
                            return dimension;
                        }
                    };
                }
            }
        };
    }
}
