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

package edu.uci.ics.hyracks.storage.am.btree.frames;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.util.BTreeUtils;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleWriterFactory;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;

public class BTreeNSMLeafFrameFactory implements ITreeIndexFrameFactory {

    private static final long serialVersionUID = 1L;
    private final ITreeIndexTupleWriterFactory tupleWriterFactory;
    private final IBinaryComparatorFactory[] cmpFactories;
    
    public BTreeNSMLeafFrameFactory(ITreeIndexTupleWriterFactory tupleWriterFactory, IBinaryComparatorFactory[] cmpFactories) {
        this.tupleWriterFactory = tupleWriterFactory;
        this.cmpFactories = cmpFactories;
    }

    @Override
    public IBTreeLeafFrame createFrame() {
    	MultiComparator cmp = BTreeUtils.createMultiComparator(cmpFactories);
        return new BTreeNSMLeafFrame(tupleWriterFactory.createTupleWriter(), cmp);
    }

    @Override
    public ITreeIndexTupleWriterFactory getTupleWriterFactory() {
        return tupleWriterFactory;
    }
}
