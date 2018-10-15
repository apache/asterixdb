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
package org.apache.hyracks.dataflow.common.data.partition.range;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.dataflow.common.utils.TaskUtil;

public class DynamicFieldRangePartitionComputerFactory extends FieldRangePartitionComputerFactory {
    private static final long serialVersionUID = 1L;
    private final String rangeMapKeyInContext;
    private final SourceLocation sourceLocation;

    public DynamicFieldRangePartitionComputerFactory(int[] rangeFields, IBinaryComparatorFactory[] comparatorFactories,
            String rangeMapKeyInContext, SourceLocation sourceLocation) {
        super(rangeFields, comparatorFactories);
        this.rangeMapKeyInContext = rangeMapKeyInContext;
        this.sourceLocation = sourceLocation;
    }

    @Override
    protected RangeMap getRangeMap(IHyracksTaskContext hyracksTaskContext) throws HyracksDataException {
        RangeMap rangeMap = TaskUtil.get(rangeMapKeyInContext, hyracksTaskContext);
        if (rangeMap == null) {
            throw HyracksDataException.create(ErrorCode.RANGEMAP_NOT_FOUND, sourceLocation);
        }
        return rangeMap;
    }
}
