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

package edu.uci.ics.asterix.om.pointables;

import edu.uci.ics.asterix.om.pointables.base.IVisitablePointable;
import edu.uci.ics.hyracks.data.std.api.IValueReference;

/**
 * This class implements several "routine" methods in IVisitablePointable
 * interface, so that subclasses do not need to repeat the same code.
 */
public abstract class AbstractVisitablePointable implements IVisitablePointable {

    private byte[] data;
    private int start = -1;
    private int len = -1;

    @Override
    public byte[] getByteArray() {
        return data;
    }

    @Override
    public int getLength() {
        return len;
    }

    @Override
    public int getStartOffset() {
        return start;
    }

    @Override
    public void set(byte[] b, int start, int len) {
        this.data = b;
        this.start = start;
        this.len = len;
    }

    @Override
    public void set(IValueReference ivf) {
        set(ivf.getByteArray(), ivf.getStartOffset(), ivf.getLength());
    }

}
