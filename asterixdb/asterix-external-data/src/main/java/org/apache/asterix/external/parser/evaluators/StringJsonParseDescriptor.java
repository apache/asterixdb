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
package org.apache.asterix.external.parser.evaluators;

import static org.apache.asterix.om.functions.BuiltinFunctions.STRING_PARSE_JSON;

import java.io.IOException;

import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * the function parse-json() accepts a string and produces an ADM value
 * Example:
 * SELECT VALUE parse_json('[1,2]')
 * <p>
 * Output:
 * [1, 2]
 */
@MissingNullInOutFunction
public class StringJsonParseDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1119187597484196172L;
    public static final IFunctionDescriptorFactory FACTORY = StringJsonParseDescriptor::new;

    @Override
    public FunctionIdentifier getIdentifier() {
        return STRING_PARSE_JSON;
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(IScalarEvaluatorFactory[] args) throws AlgebricksException {
        return new StringJsonParseEvalFactory(args[0], sourceLoc);
    }

    public static class StringJsonParseEvalFactory implements IScalarEvaluatorFactory {
        private static final long serialVersionUID = 7976257476594454552L;
        private final IScalarEvaluatorFactory stringEvalFactory;
        private final SourceLocation sourceLocation;

        public StringJsonParseEvalFactory(IScalarEvaluatorFactory stringEvalFactory, SourceLocation sourceLocation) {
            this.stringEvalFactory = stringEvalFactory;
            this.sourceLocation = sourceLocation;
        }

        @Override
        public IScalarEvaluator createScalarEvaluator(IEvaluatorContext ctx) throws HyracksDataException {
            try {
                return new StringJsonParseEval(ctx, stringEvalFactory.createScalarEvaluator(ctx), sourceLocation);
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            }
        }
    }
}
