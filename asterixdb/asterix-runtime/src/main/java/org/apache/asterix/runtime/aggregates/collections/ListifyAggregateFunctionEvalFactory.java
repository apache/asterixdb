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
package org.apache.asterix.runtime.aggregates.collections;

import java.io.IOException;

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.TypeHelper;
import org.apache.asterix.runtime.aggregates.std.AbstractAggregateFunction;
import org.apache.asterix.runtime.evaluators.functions.CastTypeEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class ListifyAggregateFunctionEvalFactory implements IAggregateEvaluatorFactory {

    private static final long serialVersionUID = 1L;
    private IScalarEvaluatorFactory[] args;
    private final AOrderedListType orderedListType;
    private final IAType itemType;
    private final SourceLocation sourceLoc;

    ListifyAggregateFunctionEvalFactory(IScalarEvaluatorFactory[] args, AOrderedListType type, IAType itemType,
            SourceLocation sourceLoc) {
        this.args = args;
        this.orderedListType = type;
        this.itemType = itemType;
        this.sourceLoc = sourceLoc;
    }

    @Override
    public IAggregateEvaluator createAggregateEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
        return new AbstractAggregateFunction(sourceLoc) {

            private IPointable inputVal = new VoidPointable();
            private IScalarEvaluator eval = args[0].createScalarEvaluator(ctx);
            private ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
            private OrderedListBuilder builder = new OrderedListBuilder();
            // create caster to open up input item if the list item type is ANY but the received item is not fully open
            private final CastTypeEvaluator caster =
                    orderedListType.getItemType().getTypeTag() == ATypeTag.ANY && !TypeHelper.isFullyOpen(itemType)
                            ? new CastTypeEvaluator(BuiltinType.ANY, itemType, eval) : null;

            @Override
            public void init() throws HyracksDataException {
                builder.reset(orderedListType);
            }

            @Override
            public void step(IFrameTupleReference tuple) throws HyracksDataException {
                try {
                    if (caster != null) {
                        caster.evaluate(tuple, inputVal);
                    } else {
                        eval.evaluate(tuple, inputVal);
                    }
                    builder.addItem(inputVal);
                } catch (IOException e) {
                    throw HyracksDataException.create(e);
                }
            }

            @Override
            public void finish(IPointable result) throws HyracksDataException {
                resultStorage.reset();
                try {
                    builder.write(resultStorage.getDataOutput(), true);
                } catch (IOException e) {
                    throw HyracksDataException.create(e);
                }
                result.set(resultStorage);
            }

            @Override
            public void finishPartial(IPointable result) throws HyracksDataException {
                finish(result);
            }

        };
    }

}
