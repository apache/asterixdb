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

import java.io.Serializable;

import org.apache.asterix.om.pointables.nonvisitor.AIntervalPointable;
import org.apache.asterix.runtime.evaluators.comparisons.ComparisonHelper;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;

public class IntervalLogic implements Serializable{

    private static final long serialVersionUID = 1L;
    private final ComparisonHelper ch = new ComparisonHelper();
    private final IPointable s1 = VoidPointable.FACTORY.createPointable();
    private final IPointable e1 = VoidPointable.FACTORY.createPointable();
    private final IPointable s2 = VoidPointable.FACTORY.createPointable();
    private final IPointable e2 = VoidPointable.FACTORY.createPointable();

    public boolean validateInterval(AIntervalPointable ip1) throws AlgebricksException {
        ip1.getStart(s1);
        ip1.getEnd(e1);
        return ch.compare(ip1.getTypeTag(), ip1.getTypeTag(), s1, e1) <= 0;
    }

    /**
     * Anything from interval 1 is less than anything from interval 2.
     *
     * @param ip1
     * @param ip2
     * @return boolean
     * @throws AlgebricksException
     * @see #after(AIntervalPointable, AIntervalPointable)
     */
    public boolean before(AIntervalPointable ip1, AIntervalPointable ip2) throws AlgebricksException {
        ip1.getEnd(e1);
        ip2.getStart(s2);
        return ch.compare(ip1.getTypeTag(), ip2.getTypeTag(), e1, s2) < 0;
    }

    public boolean after(AIntervalPointable ip1, AIntervalPointable ip2) throws AlgebricksException {
        return before(ip2, ip1);
    }

    /**
     * The end of interval 1 is the same as the start of interval 2.
     *
     * @param ip1
     * @param ip2
     * @return boolean
     * @throws AlgebricksException
     * @see #metBy(AIntervalPointable, AIntervalPointable)
     */
    public boolean meets(AIntervalPointable ip1, AIntervalPointable ip2) throws AlgebricksException {
        ip1.getEnd(e1);
        ip2.getStart(s2);
        return ch.compare(ip1.getTypeTag(), ip2.getTypeTag(), e1, s2) == 0;
    }

    public boolean metBy(AIntervalPointable ip1, AIntervalPointable ip2) throws AlgebricksException {
        return meets(ip2, ip1);
    }

    /**
     * Something at the end of interval 1 is contained as the beginning of interval 2.
     *
     * @param ip1
     * @param ip2
     * @return boolean
     * @throws AlgebricksException
     * @see #overlappedBy(AIntervalPointable, AIntervalPointable)
     */
    public boolean overlaps(AIntervalPointable ip1, AIntervalPointable ip2) throws AlgebricksException {
        ip1.getStart(s1);
        ip1.getEnd(e1);
        ip2.getStart(s2);
        ip2.getEnd(e2);
        return ch.compare(ip1.getTypeTag(), ip2.getTypeTag(), s1, s2) < 0
                && ch.compare(ip1.getTypeTag(), ip2.getTypeTag(), e1, s2) > 0
                && ch.compare(ip1.getTypeTag(), ip2.getTypeTag(), e1, e2) < 0;
    }

    public boolean overlappedBy(AIntervalPointable ip1, AIntervalPointable ip2) throws AlgebricksException {
        return overlaps(ip2, ip1);
    }

    /**
     * Something is shared by both interval 1 and interval 2.
     *
     * @param ip1
     * @param ip2
     * @throws AlgebricksException
     * @return boolean
     */
    public boolean overlap(AIntervalPointable ip1, AIntervalPointable ip2) throws AlgebricksException {
        ip1.getStart(s1);
        ip1.getEnd(e1);
        ip2.getStart(s2);
        ip2.getEnd(e2);
        return (ch.compare(ip1.getTypeTag(), ip2.getTypeTag(), s1, s2) <= 0
                && ch.compare(ip1.getTypeTag(), ip2.getTypeTag(), e1, s2) > 0)
                || (ch.compare(ip1.getTypeTag(), ip2.getTypeTag(), e1, e2) >= 0
                        && ch.compare(ip1.getTypeTag(), ip2.getTypeTag(), s1, e2) < 0);
    }

    /**
     * Anything from interval 1 is contained in the beginning of interval 2.
     *
     * @param ip1
     * @param ip2
     * @return boolean
     * @throws AlgebricksException
     * @see #startedBy(AIntervalPointable, AIntervalPointable)
     */
    public boolean starts(AIntervalPointable ip1, AIntervalPointable ip2) throws AlgebricksException {
        ip1.getStart(s1);
        ip1.getEnd(e1);
        ip2.getStart(s2);
        ip2.getEnd(e2);
        return ch.compare(ip1.getTypeTag(), ip2.getTypeTag(), s1, s2) == 0
                && ch.compare(ip1.getTypeTag(), ip2.getTypeTag(), e1, e2) <= 0;
    }

    public boolean startedBy(AIntervalPointable ip1, AIntervalPointable ip2) throws AlgebricksException {
        return starts(ip2, ip1);
    }

    /**
     * Anything from interval 2 is in interval 1.
     *
     * @param ip1
     * @param ip2
     * @return boolean
     * @throws AlgebricksException
     * @see #coveredBy(AIntervalPointable, AIntervalPointable)
     */
    public boolean covers(AIntervalPointable ip1, AIntervalPointable ip2) throws AlgebricksException {
        ip1.getStart(s1);
        ip1.getEnd(e1);
        ip2.getStart(s2);
        ip2.getEnd(e2);
        return ch.compare(ip1.getTypeTag(), ip2.getTypeTag(), s1, s2) <= 0
                && ch.compare(ip1.getTypeTag(), ip2.getTypeTag(), e1, e2) >= 0;
    }

    public boolean coveredBy(AIntervalPointable ip1, AIntervalPointable ip2) throws AlgebricksException {
        return covers(ip2, ip1);
    }

    /**
     * Anything from interval 1 is from the ending part of interval 2.
     *
     * @param ip1
     * @param ip2
     * @return boolean
     * @throws AlgebricksException
     * @see #endedBy(AIntervalPointable, AIntervalPointable)
     */
    public boolean ends(AIntervalPointable ip1, AIntervalPointable ip2) throws AlgebricksException {
        ip1.getStart(s1);
        ip1.getEnd(e1);
        ip2.getStart(s2);
        ip2.getEnd(e2);
        return ch.compare(ip1.getTypeTag(), ip2.getTypeTag(), s1, s2) >= 0
                && ch.compare(ip1.getTypeTag(), ip2.getTypeTag(), e1, e2) == 0;
    }

    public boolean endedBy(AIntervalPointable ip1, AIntervalPointable ip2) throws AlgebricksException {
        return ends(ip2, ip1);
    }

    public boolean equals(AIntervalPointable ip1, AIntervalPointable ip2) throws AlgebricksException {
        ip1.getStart(s1);
        ip1.getEnd(e1);
        ip2.getStart(s2);
        ip2.getEnd(e2);
        return ch.compare(ip1.getTypeTag(), ip2.getTypeTag(), s1, s2) == 0
                && ch.compare(ip1.getTypeTag(), ip2.getTypeTag(), e1, e2) == 0;
    }

}
