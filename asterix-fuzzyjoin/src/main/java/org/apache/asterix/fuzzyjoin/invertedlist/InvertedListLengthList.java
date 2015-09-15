/**
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

package org.apache.asterix.fuzzyjoin.invertedlist;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;

public class InvertedListLengthList implements InvertedList {
    /**
     * @author rares
     *         assumes that ListLength(s) are not empty inside the
     *         InvertedListLength
     */
    private class ListIterator implements Iterator<int[]> {

        private final Iterator<ListLength> iteratorLength;
        private Iterator<int[]> iteratorList;

        public ListIterator() {
            iteratorLength = list.iterator();
            iteratorList = null;
        }

        public boolean hasNext() {
            return (iteratorList != null && iteratorList.hasNext()) || iteratorLength.hasNext();
        }

        public int[] next() {
            if (iteratorList != null && iteratorList.hasNext()) {
                return iteratorList.next();
            }
            iteratorList = iteratorLength.next().list.iterator();
            return iteratorList.next();
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private class ListLength {
        public int length;
        public ArrayList<int[]> list = new ArrayList<int[]>();

        @Override
        public String toString() {
            StringBuffer l = new StringBuffer("(");
            for (int[] i : list) {
                l.append(Arrays.toString(i));
                l.append(",");
            }
            l.append(")");
            return "(length:" + length + "," + l + ")";
        }
    }

    private LinkedList<ListLength> list;

    public InvertedListLengthList() {
        list = new LinkedList<ListLength>();
    }

    public void add(int[] element) {
        if (!list.isEmpty() && list.getLast().length == element[2]) {
            list.getLast().list.add(element);
        } else {
            ListLength listLength = new ListLength();
            listLength.length = element[2];
            listLength.list.add(element);
            list.add(listLength);
        }
    }

    public Iterator<int[]> iterator() {
        return new ListIterator();
    }

    public void setMinLength(int minLength) {
        while (!list.isEmpty() && list.getFirst().length < minLength) {
            list.removeFirst();
        }
    }

    @Override
    public String toString() {
        return list.toString();
    }
}
