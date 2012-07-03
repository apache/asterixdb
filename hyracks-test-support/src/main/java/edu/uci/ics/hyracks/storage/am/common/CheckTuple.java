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

package edu.uci.ics.hyracks.storage.am.common;

@SuppressWarnings({"rawtypes", "unchecked"})
public class CheckTuple<T extends Comparable<T>> implements Comparable<T> {
    protected final int numKeys;
    protected final Comparable[] fields;
    protected int pos;

    public CheckTuple(int numFields, int numKeys) {
        this.numKeys = numKeys;
        this.fields = new Comparable[numFields];
        pos = 0;
    }

    public void appendField(T e) {
        fields[pos++] = e;
    }

	@Override
	public int compareTo(T o) {
		CheckTuple<T> other = (CheckTuple<T>) o;
		for (int i = 0; i < numKeys; i++) {
			int cmp = fields[i].compareTo(other.getField(i));
			if (cmp != 0) {
				return cmp;
			}
		}
		return 0;
	}

	@Override
	public boolean equals(Object o) {
		if (!(o instanceof Comparable<?>)) {
			return false;
		}
		return compareTo((T) o) == 0;
	}
    
	@Override
	public int hashCode() {
		//int hash = 37 * numKeys + fields.length;
		int hash = 0;
		for (int i = 0; i < numKeys; i++) {
			hash = 37 * hash + fields[i].hashCode();
		}
		return hash;
	}
	
	public T getField(int idx) {
		return (T) fields[idx];
	}
    
    public void setField(int idx, T e) {
        fields[idx] = e;
    }
    
    public int size() {
        return fields.length;
    }
    
    public int getNumKeys() {
        return numKeys;
    }
    
    @Override
    public String toString() {
        StringBuilder strBuilder = new StringBuilder();
        for (int i = 0; i < fields.length; i++) {
            strBuilder.append(fields[i].toString());
            if (i != fields.length-1) {
                strBuilder.append(" ");
            }
        }
        return strBuilder.toString();
    }
}