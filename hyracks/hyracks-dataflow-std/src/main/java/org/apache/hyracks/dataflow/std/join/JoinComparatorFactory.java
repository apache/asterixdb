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
package edu.uci.ics.hyracks.dataflow.std.join;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePairComparator;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePairComparatorFactory;

public class JoinComparatorFactory implements ITuplePairComparatorFactory {
    private static final long serialVersionUID = 1L;

    private final IBinaryComparatorFactory bFactory;
    private final int pos0;
    private final int pos1;

    public JoinComparatorFactory(IBinaryComparatorFactory bFactory, int pos0, int pos1) {
        this.bFactory = bFactory;
        this.pos0 = pos0;
        this.pos1 = pos1;
    }

    @Override
    public ITuplePairComparator createTuplePairComparator(IHyracksTaskContext ctx) {
        return new JoinComparator(bFactory.createBinaryComparator(), pos0, pos1);
    }

}
