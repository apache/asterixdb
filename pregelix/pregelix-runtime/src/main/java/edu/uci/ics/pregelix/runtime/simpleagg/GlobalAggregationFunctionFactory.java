/*
 * Copyright 2009-2010 by The Regents of the University of California
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

package edu.uci.ics.pregelix.runtime.simpleagg;

import java.io.DataOutput;

import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.pregelix.dataflow.base.IConfigurationFactory;
import edu.uci.ics.pregelix.dataflow.std.base.IAggregateFunction;
import edu.uci.ics.pregelix.dataflow.std.base.IAggregateFunctionFactory;

public class GlobalAggregationFunctionFactory implements IAggregateFunctionFactory {
    private static final long serialVersionUID = 1L;
    private final IConfigurationFactory confFactory;
    private final boolean isFinalStage;

    public GlobalAggregationFunctionFactory(IConfigurationFactory confFactory, boolean isFinalStage) {
        this.confFactory = confFactory;
        this.isFinalStage = isFinalStage;
    }

    @Override
    public IAggregateFunction createAggregateFunction(IDataOutputProvider provider) throws HyracksException {
        DataOutput output = provider.getDataOutput();
        return new GlobalAggregationFunction(confFactory, output, isFinalStage);
    }
}
