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
package edu.uci.ics.hyracks.algebricks.common.utils;

public class Triple<T1, T2, T3> {
    public T1 first;
    public T2 second;
    public T3 third;

    public Triple(T1 first, T2 second, T3 third) {
        this.first = first;
        this.second = second;
        this.third = third;
    }

    @Override
    public String toString() {
        return first + "," + second + ", " + third;
    }

    @Override
    public int hashCode() {
        return first.hashCode() * 31 + second.hashCode() * 15 + third.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Triple<?, ?, ?>))
            return false;
        Triple<?, ?, ?> triple = (Triple<?, ?, ?>) o;
        return first.equals(triple.first) && second.equals(triple.second) && third.equals(triple.third);
    }

}
