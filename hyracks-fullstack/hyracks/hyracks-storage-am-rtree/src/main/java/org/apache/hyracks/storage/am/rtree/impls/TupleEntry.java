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

package org.apache.hyracks.storage.am.rtree.impls;

import org.apache.hyracks.storage.am.rtree.frames.RTreeNSMFrame;

public class TupleEntry implements Comparable<TupleEntry> {
    private int tupleIndex;
    private double value;

    public TupleEntry() {
    }

    public int getTupleIndex() {
        return tupleIndex;
    }

    public void setTupleIndex(int tupleIndex) {
        this.tupleIndex = tupleIndex;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public int compareTo(TupleEntry tupleEntry) {
        double cmp = this.getValue() - tupleEntry.getValue();
        if (cmp > RTreeNSMFrame.doubleEpsilon())
            return 1;
        cmp = tupleEntry.getValue() - this.getValue();
        if (cmp > RTreeNSMFrame.doubleEpsilon())
            return -1;
        return 0;
    }
}
