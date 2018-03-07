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
package org.apache.hyracks.dataflow.std.test.util;

import org.apache.hyracks.dataflow.std.util.SelectionTree;
import org.apache.hyracks.dataflow.std.util.SelectionTree.Entry;
import org.junit.Assert;
import org.junit.Test;

public class SelectionTreeTest {
    @Test
    public void sortMergeTest() {
        SelectionTree.Entry[] entries = new SelectionTree.Entry[5];
        for (int i = 0; i < entries.length; ++i) {
            entries[i] = new MergeEntry(0, i * entries.length + i, entries.length);
        }
        SelectionTree tree = new SelectionTree(entries);
        SelectionTree.Entry e;
        int last = Integer.MIN_VALUE;
        while ((e = tree.peek()) != null) {
            MergeEntry me = (MergeEntry) e;
            if (me.i < last) {
                Assert.fail();
            }
            last = me.i;
            tree.pop();
        }
    }

    private static class MergeEntry implements SelectionTree.Entry {
        private int i;
        private int max;
        private int step;

        public MergeEntry(int min, int max, int step) {
            this.max = max;
            this.step = step;
            i = min - step;
        }

        @Override
        public int compareTo(Entry o) {
            return i < ((MergeEntry) o).i ? -1 : (i == ((MergeEntry) o).i ? 0 : 1);
        }

        @Override
        public boolean advance() {
            if (i > max) {
                return false;
            }
            i += step;
            return true;
        }
    }
}
