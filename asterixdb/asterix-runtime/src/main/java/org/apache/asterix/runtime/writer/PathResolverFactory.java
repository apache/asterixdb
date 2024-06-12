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
package org.apache.asterix.runtime.writer;

import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.EvaluatorContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class PathResolverFactory implements IPathResolverFactory {
    private static final long serialVersionUID = 8971234908711238L;
    private final IExternalFileWriterFactory writerFactory;
    private final String fileExtension;
    private final IScalarEvaluatorFactory pathEvalFactory;
    private final String staticPath;
    private final SourceLocation pathSourceLocation;

    public PathResolverFactory(IExternalFileWriterFactory writerFactory, String fileExtension,
            IScalarEvaluatorFactory pathEvalFactory, String staticPath, SourceLocation pathSourceLocation) {
        this.writerFactory = writerFactory;
        this.fileExtension = fileExtension;
        this.pathEvalFactory = pathEvalFactory;
        this.pathSourceLocation = pathSourceLocation;
        this.staticPath = staticPath;
    }

    @Override
    public IPathResolver createResolver(IHyracksTaskContext context) throws HyracksDataException {
        int partition = context.getTaskAttemptId().getTaskId().getPartition();
        char fileSeparator = writerFactory.getSeparator();
        IPathResolver resolver;
        if (staticPath == null) {
            EvaluatorContext evaluatorContext = new EvaluatorContext(context);
            IScalarEvaluator pathEval = pathEvalFactory.createScalarEvaluator(evaluatorContext);
            IWarningCollector warningCollector = context.getWarningCollector();
            resolver = new DynamicPathResolver(fileExtension, fileSeparator, partition, pathEval, warningCollector,
                    pathSourceLocation);
        } else {
            resolver = new StaticPathResolver(fileExtension, fileSeparator, partition, staticPath);
        }
        return resolver;
    }
}
