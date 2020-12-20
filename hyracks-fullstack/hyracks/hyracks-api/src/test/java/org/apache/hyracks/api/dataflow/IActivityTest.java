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
package org.apache.hyracks.api.dataflow;

import static org.apache.hyracks.api.dataflow.IActivity.DisplayNameHelper.toDisplayName;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.rewriter.runtime.SuperActivity;
import org.junit.Assert;
import org.junit.Test;

public class IActivityTest {
    @Test
    public void testDisplayName() {
        IActivity dummyActivity = new DummyActivity();
        final String displayName = dummyActivity.getDisplayName();
        Assert.assertEquals("o.a.h.a.dataflow.DummyActivity", displayName);
        Assert.assertEquals("o.a.h.a.r.runtime.SuperActivity", toDisplayName(SuperActivity.class.getName()));
        Assert.assertEquals("o.a.h.a.r.o.std.SplitOperatorDescriptor$AbstractReplicateOperatorDescriptor",
                toDisplayName("org.apache.hyracks.algebricks.runtime.operators.std.SplitOperatorDescriptor"
                        + "$AbstractReplicateOperatorDescriptor"));
    }
}

class DummyActivity implements IActivity {
    @Override
    public ActivityId getActivityId() {
        return null;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        return null;
    }
}
