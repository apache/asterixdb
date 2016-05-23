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
package org.apache.asterix.test.dataflow;

import java.util.Collections;
import java.util.HashMap;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.IPartitionWriterFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.test.FrameWriterTestUtils;
import org.apache.hyracks.api.test.TestFrameWriter;

/*
 * A partition writer factory that is used for testing partitioners
 */
public class TestPartitionWriterFactory implements IPartitionWriterFactory {
    private HashMap<Integer, TestFrameWriter> writers = new HashMap<>();

    @Override
    public IFrameWriter createFrameWriter(int receiverIndex) throws HyracksDataException {
        // The created writers must retain a deep copy of the input frame
        writers.put(receiverIndex, FrameWriterTestUtils.create(Collections.emptyList(), Collections.emptyList(), true));
        return writers.get(receiverIndex);
    }

    public HashMap<Integer, TestFrameWriter> getWriters() {
        return writers;
    }
}
