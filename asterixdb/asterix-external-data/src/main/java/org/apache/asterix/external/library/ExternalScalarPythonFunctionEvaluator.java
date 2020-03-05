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

package org.apache.asterix.external.library;

import java.util.Objects;

import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.om.functions.IExternalFunctionInfo;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.resources.IDeallocatable;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.dataflow.std.base.AbstractStateObject;

class ExternalScalarPythonFunctionEvaluator extends ExternalScalarFunctionEvaluator {

    private final PythonLibraryEvaluator libraryEvaluator;

    private final IPointable[] argValues;

    public ExternalScalarPythonFunctionEvaluator(IExternalFunctionInfo finfo, IScalarEvaluatorFactory[] args,
            IAType[] argTypes, IEvaluatorContext ctx) throws HyracksDataException {
        super(finfo, args, argTypes, ctx);
        DataverseName dataverseName = FunctionSignature.getDataverseName(finfo.getFunctionIdentifier());
        libraryEvaluator = PythonLibraryEvaluator.getInstance(dataverseName, finfo.getLibrary(), ctx.getTaskContext());
        argValues = new IPointable[args.length];
        for (int i = 0; i < argValues.length; i++) {
            argValues[i] = VoidPointable.FACTORY.createPointable();
        }
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        for (int i = 0, ln = argEvals.length; i < ln; i++) {
            argEvals[i].evaluate(tuple, argValues[i]);
        }
        PointableHelper.setNull(result);
    }

    private static class PythonLibraryEvaluator extends AbstractStateObject implements IDeallocatable {

        private PythonLibraryEvaluator(JobId jobId, PythonLibraryEvaluatorId evaluatorId) {
            super(jobId, evaluatorId);
        }

        @Override
        public void deallocate() {
        }

        private static PythonLibraryEvaluator getInstance(DataverseName dataverseName, String libraryName,
                IHyracksTaskContext ctx) {
            PythonLibraryEvaluatorId evaluatorId = new PythonLibraryEvaluatorId(dataverseName, libraryName);
            PythonLibraryEvaluator evaluator = (PythonLibraryEvaluator) ctx.getStateObject(evaluatorId);
            if (evaluator == null) {
                evaluator = new PythonLibraryEvaluator(ctx.getJobletContext().getJobId(), evaluatorId);
                ctx.registerDeallocatable(evaluator);
                ctx.setStateObject(evaluator);
            }
            return evaluator;
        }
    }

    private static final class PythonLibraryEvaluatorId {

        private final DataverseName dataverseName;

        private final String libraryName;

        private PythonLibraryEvaluatorId(DataverseName dataverseName, String libraryName) {
            this.dataverseName = Objects.requireNonNull(dataverseName);
            this.libraryName = Objects.requireNonNull(libraryName);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            PythonLibraryEvaluatorId that = (PythonLibraryEvaluatorId) o;
            return dataverseName.equals(that.dataverseName) && libraryName.equals(that.libraryName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dataverseName, libraryName);
        }
    }
}