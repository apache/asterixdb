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
package org.apache.asterix.runtime.evaluators.functions.temporal;

import org.apache.asterix.formats.nontagged.BinaryComparatorFactoryProvider;
import org.apache.asterix.om.pointables.nonvisitor.AIntervalPointable;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

class IntervalLogic {

    private final IBinaryComparator comp;
    private final ArrayBackedValueStorage s1 = new ArrayBackedValueStorage();
    private final ArrayBackedValueStorage e1 = new ArrayBackedValueStorage();
    private final ArrayBackedValueStorage s2 = new ArrayBackedValueStorage();
    private final ArrayBackedValueStorage e2 = new ArrayBackedValueStorage();

    IntervalLogic() {
        comp = BinaryComparatorFactoryProvider.INSTANCE
                .getBinaryComparatorFactory(BuiltinType.ANY, BuiltinType.ANY, true).createBinaryComparator();
    }

    /**
     * Anything from interval 1 is less than anything from interval 2.
     *
     * @param ip1 interval 1
     * @param ip2 interval 2
     * @return boolean
     * @throws HyracksDataException IOException
     * @see #after(AIntervalPointable, AIntervalPointable)
     */
    boolean before(AIntervalPointable ip1, AIntervalPointable ip2) throws HyracksDataException {
        e1.reset();
        s2.reset();
        ip1.getTaggedEnd(e1.getDataOutput());
        ip2.getTaggedStart(s2.getDataOutput());
        return comp.compare(e1.getByteArray(), e1.getStartOffset(), e1.getLength(), s2.getByteArray(),
                s2.getStartOffset(), s2.getLength()) < 0;
    }

    boolean after(AIntervalPointable ip1, AIntervalPointable ip2) throws HyracksDataException {
        return before(ip2, ip1);
    }

    /**
     * The end of interval 1 is the same as the start of interval 2.
     *
     * @param ip1 interval 1
     * @param ip2 interval 2
     * @return boolean
     * @throws HyracksDataException IOException
     * @see #metBy(AIntervalPointable, AIntervalPointable)
     */
    boolean meets(AIntervalPointable ip1, AIntervalPointable ip2) throws HyracksDataException {
        e1.reset();
        s2.reset();
        ip1.getTaggedEnd(e1.getDataOutput());
        ip2.getTaggedStart(s2.getDataOutput());
        return comp.compare(e1.getByteArray(), e1.getStartOffset(), e1.getLength(), s2.getByteArray(),
                s2.getStartOffset(), s2.getLength()) == 0;
    }

    boolean metBy(AIntervalPointable ip1, AIntervalPointable ip2) throws HyracksDataException {
        return meets(ip2, ip1);
    }

    /**
     * Something at the end of interval 1 is contained as the beginning of interval 2.
     *
     * @param ip1 interval 1
     * @param ip2 interval 2
     * @return boolean
     * @throws HyracksDataException IOException
     * @see #overlappedBy(AIntervalPointable, AIntervalPointable)
     */
    boolean overlaps(AIntervalPointable ip1, AIntervalPointable ip2) throws HyracksDataException {
        s1.reset();
        e1.reset();
        s2.reset();
        e2.reset();
        ip1.getTaggedStart(s1.getDataOutput());
        ip1.getTaggedEnd(e1.getDataOutput());
        ip2.getTaggedStart(s2.getDataOutput());
        ip2.getTaggedEnd(e2.getDataOutput());
        return comp.compare(s1.getByteArray(), s1.getStartOffset(), s1.getLength(), s2.getByteArray(),
                s2.getStartOffset(), s2.getLength()) < 0
                && comp.compare(e1.getByteArray(), e1.getStartOffset(), e1.getLength(), s2.getByteArray(),
                        s2.getStartOffset(), s2.getLength()) > 0
                && comp.compare(e1.getByteArray(), e1.getStartOffset(), e1.getLength(), e2.getByteArray(),
                        e2.getStartOffset(), e2.getLength()) < 0;
    }

    boolean overlappedBy(AIntervalPointable ip1, AIntervalPointable ip2) throws HyracksDataException {
        return overlaps(ip2, ip1);
    }

    /**
     * Something is shared by both interval 1 and interval 2.
     *
     * @param ip1 interval 1
     * @param ip2 interval 2
     * @throws HyracksDataException IOException
     * @return boolean
     */
    boolean overlapping(AIntervalPointable ip1, AIntervalPointable ip2) throws HyracksDataException {
        s1.reset();
        e1.reset();
        s2.reset();
        e2.reset();
        ip1.getTaggedStart(s1.getDataOutput());
        ip1.getTaggedEnd(e1.getDataOutput());
        ip2.getTaggedStart(s2.getDataOutput());
        ip2.getTaggedEnd(e2.getDataOutput());
        return comp.compare(s1.getByteArray(), s1.getStartOffset(), s1.getLength(), e2.getByteArray(),
                e2.getStartOffset(), e2.getLength()) < 0
                && comp.compare(e1.getByteArray(), e1.getStartOffset(), e1.getLength(), s2.getByteArray(),
                        s2.getStartOffset(), s2.getLength()) > 0;
    }

    /**
     * Anything from interval 1 is contained in the beginning of interval 2.
     *
     * @param ip1 interval 1
     * @param ip2 interval 2
     * @return boolean
     * @throws HyracksDataException IOException
     * @see #startedBy(AIntervalPointable, AIntervalPointable)
     */
    boolean starts(AIntervalPointable ip1, AIntervalPointable ip2) throws HyracksDataException {
        s1.reset();
        e1.reset();
        s2.reset();
        e2.reset();
        ip1.getTaggedStart(s1.getDataOutput());
        ip1.getTaggedEnd(e1.getDataOutput());
        ip2.getTaggedStart(s2.getDataOutput());
        ip2.getTaggedEnd(e2.getDataOutput());
        return comp.compare(s1.getByteArray(), s1.getStartOffset(), s1.getLength(), s2.getByteArray(),
                s2.getStartOffset(), s2.getLength()) == 0
                && comp.compare(e1.getByteArray(), e1.getStartOffset(), e1.getLength(), e2.getByteArray(),
                        e2.getStartOffset(), e2.getLength()) <= 0;
    }

    boolean startedBy(AIntervalPointable ip1, AIntervalPointable ip2) throws HyracksDataException {
        return starts(ip2, ip1);
    }

    /**
     * Anything from interval 2 is in interval 1.
     *
     * @param ip1 interval 1
     * @param ip2 interval 2
     * @return boolean
     * @throws HyracksDataException IOException
     * @see #coveredBy(AIntervalPointable, AIntervalPointable)
     */
    boolean covers(AIntervalPointable ip1, AIntervalPointable ip2) throws HyracksDataException {
        s1.reset();
        e1.reset();
        s2.reset();
        e2.reset();
        ip1.getTaggedStart(s1.getDataOutput());
        ip1.getTaggedEnd(e1.getDataOutput());
        ip2.getTaggedStart(s2.getDataOutput());
        ip2.getTaggedEnd(e2.getDataOutput());
        return comp.compare(s1.getByteArray(), s1.getStartOffset(), s1.getLength(), s2.getByteArray(),
                s2.getStartOffset(), s2.getLength()) <= 0
                && comp.compare(e1.getByteArray(), e1.getStartOffset(), e1.getLength(), e2.getByteArray(),
                        e2.getStartOffset(), e2.getLength()) >= 0;
    }

    boolean coveredBy(AIntervalPointable ip1, AIntervalPointable ip2) throws HyracksDataException {
        return covers(ip2, ip1);
    }

    /**
     * Anything from interval 1 is from the ending part of interval 2.
     *
     * @param ip1 interval 1
     * @param ip2 interval 2
     * @return boolean
     * @throws HyracksDataException IOException
     * @see #endedBy(AIntervalPointable, AIntervalPointable)
     */
    boolean ends(AIntervalPointable ip1, AIntervalPointable ip2) throws HyracksDataException {
        s1.reset();
        e1.reset();
        s2.reset();
        e2.reset();
        ip1.getTaggedStart(s1.getDataOutput());
        ip1.getTaggedEnd(e1.getDataOutput());
        ip2.getTaggedStart(s2.getDataOutput());
        ip2.getTaggedEnd(e2.getDataOutput());
        return comp.compare(s1.getByteArray(), s1.getStartOffset(), s1.getLength(), s2.getByteArray(),
                s2.getStartOffset(), s2.getLength()) >= 0
                && comp.compare(e1.getByteArray(), e1.getStartOffset(), e1.getLength(), e2.getByteArray(),
                        e2.getStartOffset(), e2.getLength()) == 0;
    }

    boolean endedBy(AIntervalPointable ip1, AIntervalPointable ip2) throws HyracksDataException {
        return ends(ip2, ip1);
    }
}
