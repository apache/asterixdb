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

import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * SQL++ min/max functions will mark aggregation as NULL and start to skip aggregating tuples when:
 * <ul>
 *     <li>NULL/MISSING value was encountered locally or globally</li>
 *     <li>Input data type is invalid (i.e. min/max on records) or incompatible (i.e. min/max on string & int)</li>
 * </ul>
 */
public class MinMaxAggregateFunction extends AbstractMinMaxAggregateFunction {

    MinMaxAggregateFunction(IScalarEvaluatorFactory[] args, IEvaluatorContext context, boolean isMin, Type type,
            SourceLocation sourceLoc, IAType aggFieldType) throws HyracksDataException {
        super(args, context, isMin, sourceLoc, type, aggFieldType);
    }

    @Override
    protected void processNull() {
        aggType = ATypeTag.NULL;
    }
}
