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

import java.io.UTFDataFormatException;

import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.util.string.UTF8CharBuffer;
import org.apache.hyracks.util.string.UTF8StringUtil;

final class DynamicPathResolver extends AbstractPathResolver {
    private final IScalarEvaluator pathEval;
    private final String inappropriatePartitionPath;
    private final VoidPointable pathResult;
    private final UTF8CharBuffer charBuffer;

    DynamicPathResolver(String fileExtension, char fileSeparator, int partition, long jobId, IScalarEvaluator pathEval,
            String inappropriatePartitionPath, IWarningCollector warningCollector) {
        super(fileExtension, fileSeparator, partition, jobId);
        this.pathEval = pathEval;
        this.inappropriatePartitionPath = inappropriatePartitionPath;
        pathResult = new VoidPointable();
        charBuffer = new UTF8CharBuffer();
    }

    @Override
    void appendPrefix(StringBuilder pathStringBuilder, IFrameTupleReference tuple) throws HyracksDataException {
        pathEval.evaluate(tuple, pathResult);
        if (PointableHelper.isNullOrMissing(pathResult)) {
            // TODO warn
            pathStringBuilder.append(inappropriatePartitionPath);
            return;
        }

        try {
            UTF8StringUtil.readUTF8(pathResult.getByteArray(), pathResult.getStartOffset() + 1, charBuffer);
        } catch (UTFDataFormatException e) {
            throw HyracksDataException.create(e);
        }
        pathStringBuilder.append(charBuffer.getBuffer(), 0, charBuffer.getFilledLength());
    }
}
