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

package org.apache.asterix.external.operators;

import org.apache.asterix.om.functions.IExternalFunctionDescriptor;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputPushRuntime;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputRuntimeFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public final class ExternalAssignBatchRuntimeFactory extends AbstractOneInputOneOutputRuntimeFactory {

    private static final long serialVersionUID = 1L;

    private int[] outColumns;
    private final IExternalFunctionDescriptor[] fnDescs;
    private final int[][] fnArgColumns;

    public ExternalAssignBatchRuntimeFactory(int[] outColumns, IExternalFunctionDescriptor[] fnDescs,
            int[][] fnArgColumns, int[] projectionList) {
        super(projectionList);
        this.outColumns = outColumns;
        this.fnDescs = fnDescs;
        this.fnArgColumns = fnArgColumns;
    }

    @Override
    public AbstractOneInputOneOutputPushRuntime createOneOutputPushRuntime(IHyracksTaskContext ctx)
            throws HyracksDataException {
        throw new HyracksDataException(ErrorCode.OPERATOR_NOT_IMPLEMENTED, sourceLoc,
                PhysicalOperatorTag.ASSIGN_BATCH.toString());
    }
}
