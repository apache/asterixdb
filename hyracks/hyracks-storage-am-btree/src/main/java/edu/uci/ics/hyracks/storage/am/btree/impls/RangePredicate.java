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

package edu.uci.ics.hyracks.storage.am.btree.impls;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;

public class RangePredicate implements ISearchPredicate {

    private static final long serialVersionUID = 1L;

    protected ITupleReference lowKey = null;
    protected ITupleReference highKey = null;
    protected boolean lowKeyInclusive = true;
    protected boolean highKeyInclusive = true;
    protected MultiComparator lowKeyCmp;
    protected MultiComparator highKeyCmp;

    public RangePredicate() {
    }

    public RangePredicate(ITupleReference lowKey, ITupleReference highKey, boolean lowKeyInclusive,
            boolean highKeyInclusive, MultiComparator lowKeyCmp, MultiComparator highKeyCmp) {
        this.lowKey = lowKey;
        this.highKey = highKey;
        this.lowKeyInclusive = lowKeyInclusive;
        this.highKeyInclusive = highKeyInclusive;
        this.lowKeyCmp = lowKeyCmp;
        this.highKeyCmp = highKeyCmp;
    }

    public MultiComparator getLowKeyComparator() {
        return lowKeyCmp;
    }

    public MultiComparator getHighKeyComparator() {
        return highKeyCmp;
    }

    public void setLowKeyComparator(MultiComparator lowKeyCmp) {
        this.lowKeyCmp = lowKeyCmp;
    }

    public void setHighKeyComparator(MultiComparator highKeyCmp) {
        this.highKeyCmp = highKeyCmp;
    }

    public ITupleReference getLowKey() {
        return lowKey;
    }

    public ITupleReference getHighKey() {
        return highKey;
    }

    public void setLowKey(ITupleReference lowKey, boolean lowKeyInclusive) {
        this.lowKey = lowKey;
        this.lowKeyInclusive = lowKeyInclusive;
    }

    public void setHighKey(ITupleReference highKey, boolean highKeyInclusive) {
        this.highKey = highKey;
        this.highKeyInclusive = highKeyInclusive;
    }

    public boolean isLowKeyInclusive() {
        return lowKeyInclusive;
    }

    public boolean isHighKeyInclusive() {
        return highKeyInclusive;
    }
}
