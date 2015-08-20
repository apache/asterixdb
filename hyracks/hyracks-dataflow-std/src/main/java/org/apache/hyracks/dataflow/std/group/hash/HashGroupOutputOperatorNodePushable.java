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
package edu.uci.ics.hyracks.dataflow.std.group.hash;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

class HashGroupOutputOperatorNodePushable extends AbstractUnaryOutputSourceOperatorNodePushable {
    private final IHyracksTaskContext ctx;
    private final Object stateId;

    HashGroupOutputOperatorNodePushable(IHyracksTaskContext ctx, Object stateId) {
        this.ctx = ctx;
        this.stateId = stateId;
    }

    @Override
    public void initialize() throws HyracksDataException {
        HashGroupState buildState = (HashGroupState) ctx.getStateObject(stateId);
        GroupingHashTable table = buildState.getHashTable();
        writer.open();
        try {
            table.write(writer);
        } catch (Exception e) {
            writer.fail();
            throw new HyracksDataException(e);
        } finally {
            writer.close();
        }
    }
}