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
package edu.uci.ics.asterix.runtime.evaluators.functions.temporal;

public class IntervalLogic {

    public static <T extends Comparable<T>> boolean validateInterval(T s, T e) {
        return s.compareTo(e) <= 0;
    }

    /**
     * Anything from interval 1 is less than anything from interval 2.
     * <p/>
     * |------|<br/>
     * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;|------|<br/>
     * 
     * @param s1
     * @param e1
     * @param s2
     * @param e2
     * @return
     */
    public static <T extends Comparable<T>> boolean before(T s1, T e1, T s2, T e2) {
        return e1.compareTo(s2) < 0;
    }

    public static <T extends Comparable<T>> boolean after(T s1, T e1, T s2, T e2) {
        return before(s2, e2, s1, e1);
    }

    /**
     * The end of interval 1 is the same as the start of interval 2.
     * <p/>
     * |------|<br/>
     * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;|------|<br/>
     * 
     * @param s1
     * @param e1
     * @param s2
     * @param e2
     * @return
     */
    public static <T extends Comparable<T>> boolean meets(T s1, T e1, T s2, T e2) {
        return e1.compareTo(s2) == 0;
    }

    public static <T extends Comparable<T>> boolean metBy(T s1, T e1, T s2, T e2) {
        return meets(s2, e2, s1, e1);
    }

    /**
     * Something at the end of interval 1 is contained as the beginning of interval 2.
     * <p/>
     * |------|<br/>
     * &nbsp;&nbsp;&nbsp;&nbsp;|------|<br/>
     * 
     * @param s1
     * @param e1
     * @param s2
     * @param e2
     * @return
     */
    public static <T extends Comparable<T>> boolean overlaps(T s1, T e1, T s2, T e2) {
        return s1.compareTo(s2) < 0 && e1.compareTo(s2) > 0 && e2.compareTo(e1) > 0;
    }

    public static <T extends Comparable<T>> boolean overlappedBy(T s1, T e1, T s2, T e2) {
        return overlaps(s2, e2, s1, e1);
    }

    /**
     * Something is shared by both interval 1 and interval 2.
     * <p/>
     * 
     * @param s1
     * @param e1
     * @param s2
     * @param e2
     * @return
     */
    public static <T extends Comparable<T>> boolean overlap(T s1, T e1, T s2, T e2) {
        return s1.compareTo(e2) < 0 && s2.compareTo(e1) < 0;
    }

    /**
     * Anything from interval 1 is contained in the beginning of interval 2.
     * <p/>
     * |------|<br/>
     * |-------|<br/>
     * 
     * @param s1
     * @param e1
     * @param s2
     * @param e2
     * @return
     */
    public static <T extends Comparable<T>> boolean starts(T s1, T e1, T s2, T e2) {
        return s1.compareTo(s2) == 0 && e1.compareTo(e2) <= 0;
    }

    public static <T extends Comparable<T>> boolean startedBy(T s1, T e1, T s2, T e2) {
        return starts(s2, e2, s1, e1);
    }

    /**
     * Anything from interval 2 is in interval 1.
     * <p/>
     * |------|<br/>
     * &nbsp;&nbsp;|----|<br/>
     * 
     * @param s1
     * @param e1
     * @param s2
     * @param e2
     * @return
     */
    public static <T extends Comparable<T>> boolean covers(T s1, T e1, T s2, T e2) {
        return s1.compareTo(s2) <= 0 && e1.compareTo(e2) >= 0;
    }

    public static <T extends Comparable<T>> boolean coveredBy(T s1, T e1, T s2, T e2) {
        return covers(s2, e2, s1, e1);
    }

    /**
     * Anything from interval 1 is from the ending part of interval 2.
     * <p/>
     * &nbsp;&nbsp;|-----|<br/>
     * |------|<br/>
     * 
     * @param s1
     * @param e1
     * @param s2
     * @param e2
     * @return
     */
    public static <T extends Comparable<T>> boolean ends(T s1, T e1, T s2, T e2) {
        return s1.compareTo(s2) >= 0 && e1.compareTo(e2) == 0;
    }

    public static <T extends Comparable<T>> boolean endedBy(T s1, T e1, T s2, T e2) {
        return ends(s2, e2, s1, e1);
    }

    public static <T extends Comparable<T>> boolean equals(T s1, T e1, T s2, T e2) {
        return s1.compareTo(s1) == 0 && e1.compareTo(e2) == 0;
    }

}
