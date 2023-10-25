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
import org.apache.hyracks.algebricks.runtime.writers.IExternalWriter;
import org.apache.hyracks.algebricks.runtime.writers.IExternalWriterFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;

public class ExternalWriterFactory implements IExternalWriterFactory {
    private static final long serialVersionUID = 1412969574113419638L;
    private final IExternalFileWriterFactory writerFactory;
    private final IExternalFilePrinterFactory printerFactory;
    private final String fileExtension;
    private final int maxResult;
    private final IScalarEvaluatorFactory pathEvalFactory;
    private final String inappropriatePartitionPath;
    private final String staticPath;

    public ExternalWriterFactory(IExternalFileWriterFactory writerFactory, IExternalFilePrinterFactory printerFactory,
            String fileExtension, int maxResult, IScalarEvaluatorFactory pathEvalFactory,
            String inappropriatePartitionPath, String staticPath) {
        this.writerFactory = writerFactory;
        this.printerFactory = printerFactory;
        this.fileExtension = fileExtension;
        this.maxResult = maxResult;
        this.pathEvalFactory = pathEvalFactory;
        this.inappropriatePartitionPath = inappropriatePartitionPath;
        this.staticPath = staticPath;
    }

    @Override
    public IExternalWriter createWriter(IHyracksTaskContext context) throws HyracksDataException {
        int partition = context.getTaskAttemptId().getTaskId().getPartition();
        long jobId = context.getJobletContext().getJobId().getId();
        char fileSeparator = writerFactory.getFileSeparator();
        IPathResolver resolver;
        if (staticPath == null) {
            EvaluatorContext evaluatorContext = new EvaluatorContext(context);
            IScalarEvaluator pathEval = pathEvalFactory.createScalarEvaluator(evaluatorContext);
            IWarningCollector warningCollector = context.getWarningCollector();
            resolver = new DynamicPathResolver(fileExtension, fileSeparator, partition, jobId, pathEval,
                    inappropriatePartitionPath, warningCollector);
        } else {
            resolver = new StaticPathResolver(fileExtension, fileSeparator, partition, jobId, staticPath);
        }
        IExternalFileWriter writer = writerFactory.createWriter(context, printerFactory);
        return new ExternalWriter(resolver, writer, maxResult);
    }
}
