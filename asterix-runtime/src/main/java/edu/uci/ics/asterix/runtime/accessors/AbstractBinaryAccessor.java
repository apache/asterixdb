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

package edu.uci.ics.asterix.runtime.accessors;

import edu.uci.ics.asterix.runtime.accessors.base.IBinaryAccessor;

public abstract class AbstractBinaryAccessor implements IBinaryAccessor {

    private byte[] data;
    private int start;
    private int len;
    
    @Override
    public byte[] getBytes() {
        return data;
    }

    @Override
    public int getLength() {
        return len;
    }

    @Override
    public int getStartIndex() {
        return start;
    }

    @Override
    public void reset(byte[] b, int start, int len) {
        this.data = b;
        this.start = start;
        this.len = len;
    }

}
