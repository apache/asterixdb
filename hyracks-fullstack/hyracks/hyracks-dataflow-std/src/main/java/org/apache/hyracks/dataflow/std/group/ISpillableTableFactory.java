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
package org.apache.hyracks.dataflow.std.group;

import java.io.Serializable;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public interface ISpillableTableFactory extends Serializable {

    ISpillableTable buildSpillableTable(IHyracksTaskContext ctx, int inputSizeInTuple, long dataBytesSize,
            int[] gbyFields, int[] fdFields, IBinaryComparator[] comparatorFactories,
            INormalizedKeyComputer firstKeyNormalizerFactory, IAggregatorDescriptorFactory aggregateFactory,
            RecordDescriptor inRecordDescriptor, RecordDescriptor outRecordDescriptor, int framesLimit, int seed)
            throws HyracksDataException;

}
