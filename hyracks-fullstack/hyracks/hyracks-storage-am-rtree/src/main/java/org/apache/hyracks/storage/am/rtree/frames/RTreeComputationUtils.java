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

package org.apache.hyracks.storage.am.rtree.frames;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.IPrimitiveValueProvider;
import org.apache.hyracks.storage.common.MultiComparator;

public class RTreeComputationUtils {

    public static double enlargedArea(ITupleReference tuple, ITupleReference tupleToBeInserted, MultiComparator cmp,
            IPrimitiveValueProvider[] keyValueProviders) throws HyracksDataException {
        double areaBeforeEnlarge = RTreeComputationUtils.area(tuple, cmp, keyValueProviders);
        double areaAfterEnlarge = 1.0;

        int maxFieldPos = cmp.getKeyFieldCount() / 2;
        for (int i = 0; i < maxFieldPos; i++) {
            int j = maxFieldPos + i;
            double pHigh, pLow;
            int c = cmp.getComparators()[i].compare(tuple.getFieldData(i), tuple.getFieldStart(i),
                    tuple.getFieldLength(i), tupleToBeInserted.getFieldData(i), tupleToBeInserted.getFieldStart(i),
                    tupleToBeInserted.getFieldLength(i));
            if (c < 0) {
                pLow = keyValueProviders[i].getValue(tuple.getFieldData(i), tuple.getFieldStart(i));
            } else {
                pLow = keyValueProviders[i].getValue(tupleToBeInserted.getFieldData(i),
                        tupleToBeInserted.getFieldStart(i));
            }

            c = cmp.getComparators()[j].compare(tuple.getFieldData(j), tuple.getFieldStart(j), tuple.getFieldLength(j),
                    tupleToBeInserted.getFieldData(j), tupleToBeInserted.getFieldStart(j),
                    tupleToBeInserted.getFieldLength(j));
            if (c > 0) {
                pHigh = keyValueProviders[j].getValue(tuple.getFieldData(j), tuple.getFieldStart(j));
            } else {
                pHigh = keyValueProviders[j].getValue(tupleToBeInserted.getFieldData(j),
                        tupleToBeInserted.getFieldStart(j));
            }
            areaAfterEnlarge *= pHigh - pLow;
        }
        return areaAfterEnlarge - areaBeforeEnlarge;
    }

    public static double overlappedArea(ITupleReference tuple1, ITupleReference tupleToBeInserted,
            ITupleReference tuple2, MultiComparator cmp, IPrimitiveValueProvider[] keyValueProviders)
            throws HyracksDataException {
        double area = 1.0;
        double f1, f2;

        int maxFieldPos = cmp.getKeyFieldCount() / 2;
        for (int i = 0; i < maxFieldPos; i++) {
            int j = maxFieldPos + i;
            double pHigh1, pLow1;
            if (tupleToBeInserted != null) {
                int c = cmp.getComparators()[i].compare(tuple1.getFieldData(i), tuple1.getFieldStart(i),
                        tuple1.getFieldLength(i), tupleToBeInserted.getFieldData(i), tupleToBeInserted.getFieldStart(i),
                        tupleToBeInserted.getFieldLength(i));
                if (c < 0) {
                    pLow1 = keyValueProviders[i].getValue(tuple1.getFieldData(i), tuple1.getFieldStart(i));
                } else {
                    pLow1 = keyValueProviders[i].getValue(tupleToBeInserted.getFieldData(i),
                            tupleToBeInserted.getFieldStart(i));
                }

                c = cmp.getComparators()[j].compare(tuple1.getFieldData(j), tuple1.getFieldStart(j),
                        tuple1.getFieldLength(j), tupleToBeInserted.getFieldData(j), tupleToBeInserted.getFieldStart(j),
                        tupleToBeInserted.getFieldLength(j));
                if (c > 0) {
                    pHigh1 = keyValueProviders[j].getValue(tuple1.getFieldData(j), tuple1.getFieldStart(j));
                } else {
                    pHigh1 = keyValueProviders[j].getValue(tupleToBeInserted.getFieldData(j),
                            tupleToBeInserted.getFieldStart(j));
                }
            } else {
                pLow1 = keyValueProviders[i].getValue(tuple1.getFieldData(i), tuple1.getFieldStart(i));
                pHigh1 = keyValueProviders[j].getValue(tuple1.getFieldData(j), tuple1.getFieldStart(j));
            }

            double pLow2 = keyValueProviders[i].getValue(tuple2.getFieldData(i), tuple2.getFieldStart(i));
            double pHigh2 = keyValueProviders[j].getValue(tuple2.getFieldData(j), tuple2.getFieldStart(j));

            if (pLow1 > pHigh2 || pHigh1 < pLow2) {
                return 0.0;
            }

            f1 = Math.max(pLow1, pLow2);
            f2 = Math.min(pHigh1, pHigh2);
            area *= f2 - f1;
        }
        return area;
    }

    public static double area(ITupleReference tuple, MultiComparator cmp, IPrimitiveValueProvider[] keyValueProviders) {
        double area = 1.0;
        int maxFieldPos = cmp.getKeyFieldCount() / 2;
        for (int i = 0; i < maxFieldPos; i++) {
            int j = maxFieldPos + i;
            area *= keyValueProviders[j].getValue(tuple.getFieldData(j), tuple.getFieldStart(j))
                    - keyValueProviders[i].getValue(tuple.getFieldData(i), tuple.getFieldStart(i));
        }
        return area;
    }

    public static boolean containsRegion(ITupleReference tuple1, ITupleReference tuple2, MultiComparator cmp,
            IPrimitiveValueProvider[] keyValueProviders) throws HyracksDataException {
        int maxFieldPos = cmp.getKeyFieldCount() / 2;
        for (int i = 0; i < maxFieldPos; i++) {
            int j = maxFieldPos + i;
            int c = cmp.getComparators()[i].compare(tuple1.getFieldData(i), tuple1.getFieldStart(i),
                    tuple1.getFieldLength(i), tuple2.getFieldData(i), tuple2.getFieldStart(i),
                    tuple2.getFieldLength(i));
            if (c > 0) {
                return false;
            }

            c = cmp.getComparators()[j].compare(tuple1.getFieldData(j), tuple1.getFieldStart(j),
                    tuple1.getFieldLength(j), tuple2.getFieldData(j), tuple2.getFieldStart(j),
                    tuple2.getFieldLength(j));
            if (c < 0) {
                return false;
            }
        }
        return true;
    }
}
