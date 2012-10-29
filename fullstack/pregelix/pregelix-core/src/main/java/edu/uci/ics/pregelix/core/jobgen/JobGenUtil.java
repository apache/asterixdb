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

package edu.uci.ics.pregelix.core.jobgen;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.pregelix.core.jobgen.provider.NormalizedKeyComputerFactoryProvider;
import edu.uci.ics.pregelix.core.runtime.touchpoint.WritableComparingBinaryComparatorFactory;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class JobGenUtil {

    /**
     * get normalized key factory for an iteration, to sort messages iteration
     * 1: asc order iteration 2: desc order
     * 
     * @param iteration
     * @param keyClass
     * @return
     */
    public static INormalizedKeyComputerFactory getINormalizedKeyComputerFactory(int iteration, Class keyClass) {
        return NormalizedKeyComputerFactoryProvider.INSTANCE.getAscINormalizedKeyComputerFactory(keyClass);
    }

    /**
     * get comparator for an iteration, to sort messages iteration 1: asc order
     * iteration 0: desc order
     * 
     * @param iteration
     * @param keyClass
     * @return
     */
    public static IBinaryComparatorFactory getIBinaryComparatorFactory(int iteration, Class keyClass) {
        return new WritableComparingBinaryComparatorFactory(keyClass);
    }

    /**
     * get the B-tree scan order for an iteration iteration 1: desc order,
     * backward scan iteration 2: asc order, forward scan
     * 
     * @param iteration
     * @return
     */
    public static boolean getForwardScan(int iteration) {
        return true;
    }

}
