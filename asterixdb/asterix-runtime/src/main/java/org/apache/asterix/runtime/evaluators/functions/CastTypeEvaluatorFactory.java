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

import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class CastTypeEvaluatorFactory implements IScalarEvaluatorFactory {

    private static final long serialVersionUID = 1L;

    private final IScalarEvaluatorFactory recordEvalFactory;
    private final IAType reqType;
    private final IAType inputType;
    private final SourceLocation sourceLoc;

    public CastTypeEvaluatorFactory(IScalarEvaluatorFactory recordEvalFactory, IAType reqType, IAType inputType,
            SourceLocation sourceLoc) {
        this.recordEvalFactory = recordEvalFactory;
        this.reqType = reqType;
        this.inputType = inputType;
        this.sourceLoc = sourceLoc;
    }

    @Override
    public IScalarEvaluator createScalarEvaluator(IEvaluatorContext ctx) throws HyracksDataException {
        IScalarEvaluator argEval = recordEvalFactory.createScalarEvaluator(ctx);
        return new CastTypeEvaluator(reqType, inputType, argEval, sourceLoc);
    }

    public IAType getReqType() {
        return reqType;
    }

    public IAType getInputType() {
        return inputType;
    }

    public SourceLocation getSourceLoc() {
        return sourceLoc;
    }

    public IScalarEvaluatorFactory getRecordEvalFactory() {
        return recordEvalFactory;
    }
}
