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

package edu.uci.ics.pregelix.core.data;

import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;

public class TypeTraits implements ITypeTraits {

    private static final long serialVersionUID = 1L;
    private final int length;
    private final boolean isFixedLength;

    public TypeTraits(boolean isFixedLength) {
        this.isFixedLength = isFixedLength;
        this.length = 0;
    }

    public TypeTraits(int length) {
        this.isFixedLength = true;
        this.length = length;
    }

    @Override
    public boolean isFixedLength() {
        return isFixedLength;
    }

    @Override
    public int getFixedLength() {
        return length;
    }

}
