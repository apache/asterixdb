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

import org.apache.asterix.common.exceptions.AsterixException;

/**
 * Special interval partition logic for interval partition joins.
 * Interval Partition Logic is used by the interval partition join to determine
 * which partitions have intervals that meet the given Allen's relation.
 *
 * @author prestonc
 * @see IntervalLogic
 */
public class IntervalPartitionLogic {

    /**
     * Anything from interval 1 is less than anything from interval 2.
     *
     * @param s1
     *            First interval start partition
     * @param e1
     *            First interval end partition
     * @param s2
     *            Second interval start partition
     * @param e2
     *            Second interval end partition
     * @return boolean
     * @throws AsterixException
     * @see #after(int, int, int, int)
     */
    public static boolean before(int s1, int e1, int s2, int e2) {
        return e1 <= s2;
    }

    public static boolean after(int s1, int e1, int s2, int e2) {
        return before(s2, e2, s1, e1);
    }

    /**
     * The end of interval 1 is the same as the start of interval 2.
     *
     * @param s1
     *            First interval start partition
     * @param e1
     *            First interval end partition
     * @param s2
     *            Second interval start partition
     * @param e2
     *            Second interval end partition
     * @return boolean
     * @see #metBy(int, int, int, int)
     */
    public static boolean meets(int s1, int e1, int s2, int e2) {
        return e1 == s2;
    }

    public static boolean metBy(int s1, int e1, int s2, int e2) {
        return meets(s2, e2, s1, e1);
    }

    /**
     * Something at the end of interval 1 is contained as the beginning of interval 2.
     *
     * @param s1
     *            First interval start partition
     * @param e1
     *            First interval end partition
     * @param s2
     *            Second interval start partition
     * @param e2
     *            Second interval end partition
     * @return boolean
     * @see #overlappedBy(int, int, int, int)
     */
    public static boolean overlaps(int s1, int e1, int s2, int e2) {
        return s1 <= s2 && e1 >= s2 && e1 <= e2;
    }

    public static boolean overlappedBy(int s1, int e1, int s2, int e2) {
        return overlaps(s2, e2, s1, e1);
    }

    /**
     * Something is shared by both interval 1 and interval 2.
     *
     * @param s1
     *            First interval start partition
     * @param e1
     *            First interval end partition
     * @param s2
     *            Second interval start partition
     * @param e2
     *            Second interval end partition
     * @return boolean
     */
    public static boolean overlapping(int s1, int e1, int s2, int e2) {
        return s1 <= e2 && e1 >= s2;
    }

    /**
     * Anything from interval 1 is contained in the beginning of interval 2.
     *
     * @param ip1
     * @param ip2
     * @return boolean
     * @throws AsterixException
     * @see #startedBy(int, int, int, int)
     */
    public static boolean starts(int s1, int e1, int s2, int e2) {
        return s1 == s2 && e1 <= e2;
    }

    public static boolean startedBy(int s1, int e1, int s2, int e2) {
        return starts(s2, e2, s1, e1);
    }

    /**
     * Anything from interval 2 is in interval 1.
     *
     * @param s1
     *            First interval start partition
     * @param e1
     *            First interval end partition
     * @param s2
     *            Second interval start partition
     * @param e2
     *            Second interval end partition
     * @return boolean
     * @see #coveredBy(int, int, int, int)
     */
    public static boolean covers(int s1, int e1, int s2, int e2) {
        return s1 <= s2 && e1 >= e2;
    }

    public static boolean coveredBy(int s1, int e1, int s2, int e2) {
        return covers(s2, e2, s1, e1);
    }

    /**
     * Anything from interval 1 is from the ending part of interval 2.
     *
     * @param s1
     *            First interval start partition
     * @param e1
     *            First interval end partition
     * @param s2
     *            Second interval start partition
     * @param e2
     *            Second interval end partition
     * @return boolean
     * @see #endedBy(int, int, int, int)
     */
    public static boolean ends(int s1, int e1, int s2, int e2) {
        return s1 >= s2 && e1 == e2;
    }

    public static boolean endedBy(int s1, int e1, int s2, int e2) {
        return ends(s2, e2, s1, e1);
    }

    public static boolean equals(int s1, int e1, int s2, int e2) {
        return s1 == s2 && e1 == e2;
    }
}
