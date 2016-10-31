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
package org.apache.asterix.runtime.aggregates.std;

import java.io.IOException;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.exceptions.UnsupportedItemTypeException;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class MinMaxAggregateFunction extends AbstractMinMaxAggregateFunction {
    private final boolean isLocalAgg;

    public MinMaxAggregateFunction(IScalarEvaluatorFactory[] args, IHyracksTaskContext context, boolean isMin,
            boolean isLocalAgg) throws HyracksDataException {
        super(args, context, isMin);
        this.isLocalAgg = isLocalAgg;
    }

    @Override
    protected void processNull() {
        aggType = ATypeTag.NULL;
    }

    @Override
    protected boolean skipStep() {
        return aggType == ATypeTag.NULL;
    }

    @Override
    protected void processSystemNull() throws HyracksDataException {
        if (isLocalAgg) {
            throw new UnsupportedItemTypeException("min/max", ATypeTag.SERIALIZED_SYSTEM_NULL_TYPE_TAG);
        }
    }

    @Override
    protected void finishSystemNull() throws IOException {
        // Empty stream. For local agg return system null. For global agg return null.
        if (isLocalAgg) {
            resultStorage.getDataOutput().writeByte(ATypeTag.SERIALIZED_SYSTEM_NULL_TYPE_TAG);
        } else {
            resultStorage.getDataOutput().writeByte(ATypeTag.SERIALIZED_NULL_TYPE_TAG);
        }
    }

}
