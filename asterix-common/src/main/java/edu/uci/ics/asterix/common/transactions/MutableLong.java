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
package edu.uci.ics.asterix.common.transactions;

public class MutableLong {
    private long val;

    
    public MutableLong() {
    }
    
    public MutableLong(long val) {
        this.val = val;
    }
    
    public void set(long val) {
        this.val = val;
    }

    public long get() {
        return val;
    }
    
    @Override
    public int hashCode() {
        return (int)val;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof MutableLong)) {
            return false;
        }
        return ((MutableLong) o).val == val;
    }

}
