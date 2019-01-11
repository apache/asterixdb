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

package org.apache.asterix.runtime.runningaggregates.std;

import java.io.DataOutput;

import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * Evaluator for {@code percent_rank()} window function
 */
class PercentRankRunningAggregateEvaluator extends AbstractRankRunningAggregateEvaluator {

    private final AMutableDouble aDouble = new AMutableDouble(0);

    @SuppressWarnings("unchecked")
    private final ISerializerDeserializer<ADouble> serde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADOUBLE);

    private double divisor;

    PercentRankRunningAggregateEvaluator(IScalarEvaluator[] args, SourceLocation sourceLoc) {
        super(args, false, sourceLoc);
    }

    @Override
    public void initPartition(long partitionLength) {
        super.initPartition(partitionLength);
        divisor = (double) partitionLength - 1;
    }

    @Override
    protected void computeResult(DataOutput out) throws HyracksDataException {
        double percentRank = first ? 0 : (rank - 1) / divisor;
        aDouble.setValue(percentRank);
        serde.serialize(aDouble, out);
    }
}
