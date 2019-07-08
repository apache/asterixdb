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
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * SQL min/max functions will mark aggregation as NULL and start to skip aggregating tuples when:
 * <ul>
 *     <li>NULL/MISSING value was encountered globally</li>
 *     <li>Input data type is invalid (i.e. min/max on records) or incompatible (i.e. min/max on string & int)</li>
 * </ul>
 * When NULL/MISSING value is encountered, local aggregator will ignore and continue aggregation. Global aggregator
 * will mark aggregation as NULL since getting NULL/MISSING at the global level indicates type invalidity (Some
 * aggregators are global in nature yet they ignore NULLs similar to a local aggregator, e.g. scalar min/max, normal
 * global aggregators with no local aggregators like distinct min/max (one-step aggregators)).
 */
public class SqlMinMaxAggregateFunction extends AbstractMinMaxAggregateFunction {

    SqlMinMaxAggregateFunction(IScalarEvaluatorFactory[] args, IEvaluatorContext context, boolean isMin, Type type,
            SourceLocation sourceLoc, IAType aggFieldType) throws HyracksDataException {
        super(args, context, isMin, sourceLoc, type, aggFieldType);
    }

    @Override
    protected void processNull() {
        if (type == Type.GLOBAL || type == Type.INTERMEDIATE) {
            // getting NULL at the global step should only mean the local step ran into type invalidity
            aggType = ATypeTag.NULL;
        }
    }
}
