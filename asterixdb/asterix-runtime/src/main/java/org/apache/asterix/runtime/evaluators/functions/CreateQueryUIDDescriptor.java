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
package org.apache.asterix.runtime.evaluators.functions;

import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

/**
 * Create global unique id within a query.
 */
public class CreateQueryUIDDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new CreateQueryUIDDescriptor();
        }
    };

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;
            private static final int BINARY_LENGTH = 14;
            private static final int PAYLOAD_START = 2;

            @Override
            public IScalarEvaluator createScalarEvaluator(IEvaluatorContext ctx) throws HyracksDataException {
                // Format: |TypeTag | PayloadLength | Payload |
                // TypeTag: 1 byte
                // PayloadLength: 1 byte
                // Payload: 12 bytes:  |partition-id (4 bytes) | local-id (8 bytes) |
                byte[] uidBytes = new byte[BINARY_LENGTH];
                // Writes the type tag.
                uidBytes[0] = ATypeTag.SERIALIZED_BINARY_TYPE_TAG;
                // Writes the payload size.
                uidBytes[1] = BINARY_LENGTH - PAYLOAD_START;
                // Writes the 4 byte partition id.
                IntegerPointable.setInteger(uidBytes, PAYLOAD_START,
                        ctx.getTaskContext().getTaskAttemptId().getTaskId().getPartition());

                return new IScalarEvaluator() {

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        // Increments the Unique ID value.
                        for (int i = BINARY_LENGTH - 1; i >= PAYLOAD_START; i--) {
                            if (++uidBytes[i] != 0) {
                                break;
                            }
                        }
                        result.set(uidBytes, 0, BINARY_LENGTH);
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.CREATE_QUERY_UID;
    }

}
