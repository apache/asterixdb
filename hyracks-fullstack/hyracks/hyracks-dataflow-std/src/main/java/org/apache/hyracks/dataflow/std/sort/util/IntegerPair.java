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
package org.apache.hyracks.dataflow.std.sort.util;

import java.io.Serializable;
import java.util.Comparator;

public class IntegerPair implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final Comparator<IntegerPair> LEFT_ASC_COMPARATOR = new Comparator<IntegerPair>() {
        @Override
        public int compare(IntegerPair p1, IntegerPair p2) {
            return p1.getLeft() - p2.getLeft();
        }

    };

    public static final Comparator<IntegerPair> RIGHT_ASC_COMPARATOR = new Comparator<IntegerPair>() {
        @Override
        public int compare(IntegerPair p1, IntegerPair p2) {
            return p1.getRight() - p2.getRight();
        }

    };

    private int left;
    private int right;

    public IntegerPair() {
        reset(Integer.MIN_VALUE, Integer.MIN_VALUE);
    }

    public IntegerPair(int l, int r) {
        reset(l, r);
    }

    public int getLeft() {
        return left;
    }

    public int getRight() {
        return right;
    }

    public void reset(int l, int r) {
        left = l;
        right = r;
    }

    @Override
    public String toString() {
        return left + "," + right;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof IntegerPair)) {
            return false;
        } else {
            IntegerPair p = (IntegerPair) obj;
            return this.left == p.getLeft() && this.right == p.getRight();
        }
    }

    @Override
    public int hashCode() {
        return left * 31 + right;
    }

}
