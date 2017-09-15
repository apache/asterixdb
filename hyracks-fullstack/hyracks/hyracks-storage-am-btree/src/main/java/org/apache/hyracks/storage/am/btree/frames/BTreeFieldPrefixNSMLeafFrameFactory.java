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

package org.apache.hyracks.storage.am.btree.frames;

import org.apache.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import org.apache.hyracks.storage.am.btree.tuples.BTreeTypeAwareTupleWriterFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;

public class BTreeFieldPrefixNSMLeafFrameFactory implements ITreeIndexFrameFactory {
    private static final long serialVersionUID = 1L;

    protected final BTreeTypeAwareTupleWriterFactory tupleWriterFactory;

    public BTreeFieldPrefixNSMLeafFrameFactory(BTreeTypeAwareTupleWriterFactory tupleWriterFactory) {
        this.tupleWriterFactory = tupleWriterFactory;
    }

    @Override
    public IBTreeLeafFrame createFrame() {
        return new BTreeFieldPrefixNSMLeafFrame(tupleWriterFactory.createTupleWriter());
    }

    @Override
    public BTreeTypeAwareTupleWriterFactory getTupleWriterFactory() {
        return tupleWriterFactory;
    }
}
