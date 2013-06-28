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

package edu.uci.ics.asterix.dataflow.data.nontagged.keynormalizers;

import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputer;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;

/**
 * This class uses a decorator pattern to wrap an ASC ordered INomralizedKeyComputerFactory implementation to
 * obtain the ASC order.
 */
public class AWrappedAscNormalizedKeyComputerFactory implements INormalizedKeyComputerFactory {

    private static final long serialVersionUID = 1L;
    private final INormalizedKeyComputerFactory nkcf;

    public AWrappedAscNormalizedKeyComputerFactory(INormalizedKeyComputerFactory nkcf) {
        this.nkcf = nkcf;
    }

    @Override
    public INormalizedKeyComputer createNormalizedKeyComputer() {
        final INormalizedKeyComputer nkc = nkcf.createNormalizedKeyComputer();
        return new INormalizedKeyComputer() {

            @Override
            public int normalize(byte[] bytes, int start, int length) {
                // start +1, length -1 is because in ASTERIX data format, there is always a type tag before the value 
                return nkc.normalize(bytes, start + 1, length - 1);
            }
        };
    }

}
