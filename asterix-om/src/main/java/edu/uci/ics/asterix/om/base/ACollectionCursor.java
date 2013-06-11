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
package edu.uci.ics.asterix.om.base;

import java.util.ArrayList;

public class ACollectionCursor implements IACursor {

    private ArrayList<IAObject> values;
    private int pos;
    private int size;
    private IAObject object = null;

    public ACollectionCursor() {
    }

    public ACollectionCursor(AUnorderedList bag) {
        reset(bag);
    }

    public ACollectionCursor(AOrderedList list) {
        reset(list);
    }

    @Override
    public boolean next() {
        pos++;
        if (pos < size) {
            object = values.get(pos);
            return true;
        } else {
            object = null;
            return false;
        }
    }

    @Override
    public IAObject get() { // returns the current object
        return object;
    }

    @Override
    public void reset() {
        pos = -1;
        size = values.size();
    }

    public void reset(AUnorderedList bag) {
        this.values = bag.values;
        reset();
    }

    public void reset(AOrderedList list) {
        this.values = list.values;
        reset();
    }

}