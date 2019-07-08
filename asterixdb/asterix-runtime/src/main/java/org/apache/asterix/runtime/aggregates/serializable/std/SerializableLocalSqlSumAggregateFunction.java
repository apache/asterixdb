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
package org.apache.asterix.runtime.aggregates.serializable.std;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.exceptions.UnsupportedItemTypeException;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class SerializableLocalSqlSumAggregateFunction extends AbstractSerializableSumAggregateFunction {

    public SerializableLocalSqlSumAggregateFunction(IScalarEvaluatorFactory[] args, IEvaluatorContext context,
            SourceLocation sourceLoc) throws HyracksDataException {
        super(args, context, sourceLoc);
    }

    // Called for each incoming tuple
    @Override
    public void step(IFrameTupleReference tuple, byte[] state, int start, int len) throws HyracksDataException {
        super.step(tuple, state, start, len);
    }

    // Finish calculation
    @Override
    public void finish(byte[] state, int start, int len, DataOutput out) throws HyracksDataException {
        super.finishPartial(state, start, len, out);
    }

    // Is skip
    @Override
    protected boolean skipStep(byte[] state, int start) {
        return false;
    }

    // Handle NULL step
    @Override
    protected void processNull(byte[] state, int start) {
        // Do nothing
    }

    // Handle SYSTEM_NULL step
    @Override
    protected void processSystemNull() throws HyracksDataException {
        throw new UnsupportedItemTypeException(sourceLoc, BuiltinFunctions.SUM,
                ATypeTag.SERIALIZED_SYSTEM_NULL_TYPE_TAG);
    }

    // Handle NULL finish
    @Override
    protected void finishNull(DataOutput out) throws IOException {
        out.writeByte(ATypeTag.SERIALIZED_NULL_TYPE_TAG);
    }

    // Handle SYSTEM_NULL finish
    @Override
    protected void finishSystemNull(DataOutput out) throws IOException {
        out.writeByte(ATypeTag.SERIALIZED_SYSTEM_NULL_TYPE_TAG);
    }
}
