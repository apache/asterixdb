/**
 * Copyright 2010-2011 The Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on
 * an "AS IS"; BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * Author: Rares Vernica <rares (at) ics.uci.edu>
 */

package edu.uci.ics.asterix.fuzzyjoin;

public class MutableInteger implements Comparable {
    private int v;

    public MutableInteger(int v) {
        this.v = v;
    }

    @Override
    public int compareTo(Object o) {
        MutableInteger m = (MutableInteger) o;
        return v - m.v;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (o == this) {
            return true;
        }
        if (!(o instanceof MutableInteger)) {
            return false;
        }
        MutableInteger m = (MutableInteger) o;
        if (m.v == v) {
            return true;
        }
        return false;
    }

    public int get() {
        return v;
    }

    public void inc() {
        v += 1;
    }

    public void set(int v) {
        this.v = v;
    }

    @Override
    public String toString() {
        return "" + v;
    }

}
