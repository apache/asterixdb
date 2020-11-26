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
package org.apache.asterix.external.library.adapter;

import org.apache.asterix.external.dataflow.AbstractFeedDataFlowController;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.storage.am.common.api.ITupleFilter;

class TestTypedFeedDataFlowController extends AbstractFeedDataFlowController {
    TestTypedFeedDataFlowController(IHyracksTaskContext ctx) {
        super(ctx, null, 0);
    }

    @Override
    public String getStats() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void start(IFrameWriter writer, ITupleFilter tupleFilter, long outputLimit) {
        throw new UnsupportedOperationException();
    }
}
