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

/**
 * Special interval partition logic for interval partition joins.
 *
 * Interval Partition Logic is used by the interval partition join to determine
 * which partitions have intervals that meet the given Allen's relation.
 *
 * @author prestonc
 */
public class IntervalPartitionLogic extends IntervalLogic {

    /**
     * Anything from interval 1 is less than anything from interval 2.
     *
     * @param s1
     *            First interval start point
     * @param e1
     *            First interval end point
     * @param s2
     *            Second interval start point
     * @param e2
     *            Second interval end point
     * @return boolean
     * @see #after(Comparable, Comparable, Comparable, Comparable)
     */
    public static <T extends Comparable<T>> boolean before(T s1, T e1, T s2, T e2) {
        return e1.compareTo(s2) <= 0;
    }

    /**
     * Something at the end of interval 1 is contained as the beginning of interval 2.
     *
     * @param s1
     *            First interval start point
     * @param e1
     *            First interval end point
     * @param s2
     *            Second interval start point
     * @param e2
     *            Second interval end point
     * @return boolean
     * @see #overlappedBy(Comparable, Comparable, Comparable, Comparable)
     */
    public static <T extends Comparable<T>> boolean overlaps(T s1, T e1, T s2, T e2) {
        return s1.compareTo(s2) <= 0 && e1.compareTo(s2) >= 0 && e2.compareTo(e1) >= 0;
    }

    /**
     * Something is shared by both interval 1 and interval 2.
     *
     * @param s1
     *            First interval start point
     * @param e1
     *            First interval end point
     * @param s2
     *            Second interval start point
     * @param e2
     *            Second interval end point
     * @return boolean
     */
    public static <T extends Comparable<T>> boolean overlapping(T s1, T e1, T s2, T e2) {
        return s1.compareTo(e2) <= 0 && e1.compareTo(s2) >= 0;
    }

    /**
     * Anything from interval 2 is in interval 1.
     *
     * @param s1
     *            First interval start point
     * @param e1
     *            First interval end point
     * @param s2
     *            Second interval start point
     * @param e2
     *            Second interval end point
     * @return boolean
     * @see #coveredBy(Comparable, Comparable, Comparable, Comparable)
     */
    public static <T extends Comparable<T>> boolean covers(T s1, T e1, T s2, T e2) {
        return s1.compareTo(s2) <= 0 && e1.compareTo(e2) >= 0;
    }
}
