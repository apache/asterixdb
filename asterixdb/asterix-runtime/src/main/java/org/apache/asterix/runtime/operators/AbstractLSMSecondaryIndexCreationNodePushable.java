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
package org.apache.asterix.runtime.operators;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.BooleanPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;

public abstract class AbstractLSMSecondaryIndexCreationNodePushable
        extends AbstractUnaryInputUnaryOutputOperatorNodePushable {
    protected final IHyracksTaskContext ctx;
    protected final RecordDescriptor inputRecDesc;

    protected final int partition;
    protected final int numTagFields;
    protected final int numSecondaryKeys;
    protected final int numPrimaryKeys;

    protected final boolean hasBuddyBTree;

    protected FrameTupleAccessor accessor;

    public AbstractLSMSecondaryIndexCreationNodePushable(IHyracksTaskContext ctx, int partition,
            RecordDescriptor inputRecDesc, int numTagFields, int numSecondaryKeys, int numPrimaryKeys,
            boolean hasBuddyBTree) {
        this.ctx = ctx;
        this.inputRecDesc = inputRecDesc;

        this.partition = partition;
        this.numTagFields = numTagFields;
        this.numSecondaryKeys = numSecondaryKeys;
        this.numPrimaryKeys = numPrimaryKeys;
        this.hasBuddyBTree = hasBuddyBTree;

    }

    @Override
    public void open() throws HyracksDataException {
        accessor = new FrameTupleAccessor(inputRecDesc);
        try {
            writer.open();
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        writer.fail();
    }

    protected int getComponentPos(ITupleReference tuple) {
        return IntegerPointable.getInteger(tuple.getFieldData(0), tuple.getFieldStart(0));
    }

    protected boolean isAntiMatterTuple(ITupleReference tuple) {
        return BooleanPointable.getBoolean(tuple.getFieldData(1), tuple.getFieldStart(1));
    }

}