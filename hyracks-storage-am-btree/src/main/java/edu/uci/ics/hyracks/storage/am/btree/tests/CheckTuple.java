/*
 * Copyright 2009-2010 by The Regents of the University of California
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

package edu.uci.ics.hyracks.storage.am.btree.tests;

@SuppressWarnings({"rawtypes", "unchecked"})
public class CheckTuple<T extends Comparable<T>> implements Comparable<T> {
    private final int numKeys;    
    private final Comparable[] tuple;
    private int pos;

    public CheckTuple(int numFields, int numKeys) {
        this.numKeys = numKeys;
        this.tuple = new Comparable[numFields];
        pos = 0;
    }

    public void add(T e) {
        tuple[pos++] = e;
    }

    @Override
    public int compareTo(T o) {
        CheckTuple<T> other = (CheckTuple<T>)o;
        for (int i = 0; i < numKeys; i++) {            
            int cmp = tuple[i].compareTo(other.get(i));
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    public T get(int idx) {
        return (T)tuple[idx];
    }
    
    public void set(int idx, T e) {
        tuple[idx] = e;
    }
    
    public int size() {
        return tuple.length;
    }
    
    public int getNumKeys() {
        return numKeys;
    }
    
    @Override
    public String toString() {
        StringBuilder strBuilder = new StringBuilder();
        for (int i = 0; i < tuple.length; i++) {
            strBuilder.append(tuple[i].toString());
            if (i != tuple.length-1) {
                strBuilder.append(" ");
            }
        }
        return strBuilder.toString();
    }
}