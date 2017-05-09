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
package org.apache.asterix.om.base;

import java.util.List;

public class ACollectionCursor implements IACursor {

    private List<IAObject> values;
    private int pos;
    private int size;
    private IAObject object = null;

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
