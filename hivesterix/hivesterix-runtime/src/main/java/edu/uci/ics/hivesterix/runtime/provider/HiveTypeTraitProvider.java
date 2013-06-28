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
package edu.uci.ics.hivesterix.runtime.provider;

import java.io.Serializable;

import edu.uci.ics.hyracks.algebricks.data.ITypeTraitProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;

public class HiveTypeTraitProvider implements ITypeTraitProvider, Serializable {
    private static final long serialVersionUID = 1L;
    public static HiveTypeTraitProvider INSTANCE = new HiveTypeTraitProvider();

    private HiveTypeTraitProvider() {

    }

    @Override
    public ITypeTraits getTypeTrait(Object arg0) {
        return new ITypeTraits() {
            private static final long serialVersionUID = 1L;

            @Override
            public int getFixedLength() {
                return -1;
            }

            @Override
            public boolean isFixedLength() {
                return false;
            }

        };
    }
}
