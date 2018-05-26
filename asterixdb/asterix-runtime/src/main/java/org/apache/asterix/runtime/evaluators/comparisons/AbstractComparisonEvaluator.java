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
package org.apache.asterix.runtime.evaluators.comparisons;

import java.io.DataOutput;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.TaggedValuePointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public abstract class AbstractComparisonEvaluator implements IScalarEvaluator {

    protected final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    protected final DataOutput out = resultStorage.getDataOutput();
    protected final TaggedValuePointable argLeft =
            (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
    protected final TaggedValuePointable argRight =
            (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
    protected final IPointable outLeft = VoidPointable.FACTORY.createPointable();
    protected final IPointable outRight = VoidPointable.FACTORY.createPointable();
    protected final IScalarEvaluator evalLeft;
    protected final IScalarEvaluator evalRight;
    protected final SourceLocation sourceLoc;
    private final ComparisonHelper ch;

    public AbstractComparisonEvaluator(IScalarEvaluator evalLeft, IScalarEvaluator evalRight,
            SourceLocation sourceLoc) {
        this.evalLeft = evalLeft;
        this.evalRight = evalRight;
        this.sourceLoc = sourceLoc;
        ch = new ComparisonHelper(sourceLoc);
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        // Evaluates input args.
        evalLeft.evaluate(tuple, argLeft);
        evalRight.evaluate(tuple, argRight);
        argLeft.getValue(outLeft);
        argRight.getValue(outRight);

        evaluateImpl(result);
    }

    protected abstract void evaluateImpl(IPointable result) throws HyracksDataException;

    // checks whether two types are comparable
    boolean comparabilityCheck() {
        // Checks whether two types are comparable or not
        ATypeTag typeTag1 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argLeft.getTag());
        ATypeTag typeTag2 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argRight.getTag());

        // Are two types compatible, meaning that they can be compared? (e.g., compare between numeric types
        return ATypeHierarchy.isCompatible(typeTag1, typeTag2);
    }

    int compare() throws HyracksDataException {
        return ch.compare(EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argLeft.getTag()),
                EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argRight.getTag()), outLeft, outRight);
    }
}
