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

package edu.uci.ics.hyracks.storage.am.rtree.frames;

import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleWriterFactory;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeFrame;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeFrameFactory;

public class NSMRTreeFrameFactory implements IRTreeFrameFactory {

    private static final long serialVersionUID = 1L;
    private ITreeIndexTupleWriterFactory tupleWriterFactory;

    public NSMRTreeFrameFactory(ITreeIndexTupleWriterFactory tupleWriterFactory) {
        this.tupleWriterFactory = tupleWriterFactory;
    }

    @Override
    public IRTreeFrame getFrame() {
        return new NSMRTreeFrame(tupleWriterFactory.createTupleWriter());
    }
}
