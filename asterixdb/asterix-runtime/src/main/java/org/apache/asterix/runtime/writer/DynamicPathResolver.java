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

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.util.string.UTF8CharBuffer;
import org.apache.hyracks.util.string.UTF8StringUtil;

final class DynamicPathResolver extends AbstractPathResolver {
    private final IScalarEvaluator pathEval;
    private final IWarningCollector warningCollector;
    private final StringBuilder dirStringBuilder;
    private final VoidPointable pathResult;
    private final UTF8CharBuffer charBuffer;
    private final SourceLocation pathSourceLocation;

    DynamicPathResolver(String fileExtension, char fileSeparator, int partition, IScalarEvaluator pathEval,
            IWarningCollector warningCollector, SourceLocation pathSourceLocation) {
        super(fileExtension, fileSeparator, partition);
        this.pathEval = pathEval;
        this.warningCollector = warningCollector;
        this.pathSourceLocation = pathSourceLocation;
        dirStringBuilder = new StringBuilder();
        pathResult = new VoidPointable();
        charBuffer = new UTF8CharBuffer();
    }

    @Override
    public String getPartitionDirectory(IFrameTupleReference tuple) throws HyracksDataException {
        if (!appendPrefix(tuple)) {
            return ExternalWriter.UNRESOLVABLE_PATH;
        }

        if (dirStringBuilder.length() > 0 && dirStringBuilder.charAt(dirStringBuilder.length() - 1) != fileSeparator) {
            dirStringBuilder.append(fileSeparator);
        }
        return dirStringBuilder.toString();
    }

    private boolean appendPrefix(IFrameTupleReference tuple) throws HyracksDataException {
        dirStringBuilder.setLength(0);
        pathEval.evaluate(tuple, pathResult);
        ATypeTag typeTag = ATypeTag.VALUE_TYPE_MAPPING[pathResult.getByteArray()[0]];
        if (typeTag != ATypeTag.STRING) {
            if (warningCollector.shouldWarn()) {
                warningCollector.warn(Warning.of(pathSourceLocation, ErrorCode.NON_STRING_WRITE_PATH, typeTag));
            }
            return false;
        }

        try {
            UTF8StringUtil.readUTF8(pathResult.getByteArray(), pathResult.getStartOffset() + 1, charBuffer);
        } catch (UTFDataFormatException e) {
            throw HyracksDataException.create(e);
        }
        dirStringBuilder.append(charBuffer.getBuffer(), 0, charBuffer.getFilledLength());
        return true;
    }
}
