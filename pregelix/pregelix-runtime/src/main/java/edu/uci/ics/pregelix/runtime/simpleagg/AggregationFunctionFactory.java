/*
 * Copyright 2009-2013 by The Regents of the University of California
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

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.pregelix.dataflow.base.IConfigurationFactory;
import edu.uci.ics.pregelix.dataflow.std.base.IAggregateFunction;
import edu.uci.ics.pregelix.dataflow.std.base.IAggregateFunctionFactory;

public class AggregationFunctionFactory implements IAggregateFunctionFactory {
    private static final long serialVersionUID = 1L;
    private final IConfigurationFactory confFactory;
    private final boolean isFinalStage;
    private final boolean partialAggAsInput;

    public AggregationFunctionFactory(IConfigurationFactory confFactory, boolean isFinalStage, boolean partialAggAsInput) {
        this.confFactory = confFactory;
        this.isFinalStage = isFinalStage;
        this.partialAggAsInput = partialAggAsInput;
    }

    @Override
    public IAggregateFunction createAggregateFunction(IHyracksTaskContext ctx, IDataOutputProvider provider,
            IFrameWriter writer) throws HyracksException {
        DataOutput output = provider.getDataOutput();
        return new AggregationFunction(ctx, confFactory, output, writer, isFinalStage, partialAggAsInput);
    }
}
