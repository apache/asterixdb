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

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.parser.JSONDataParser;
import org.apache.asterix.external.parser.factory.JSONDataParserFactory;
import org.apache.asterix.external.provider.context.DefaultExternalRuntimeDataContext;
import org.apache.asterix.om.exceptions.ExceptionUtil;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleInputStream;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class StringJsonParseEval implements IScalarEvaluator {
    private final IEvaluatorContext ctx;
    private final IScalarEvaluator inputEval;
    private final JSONDataParser parser;
    private final SourceLocation sourceLocation;
    private final IPointable inputVal;
    private final UTF8StringPointable utf8Val;
    private final ByteArrayAccessibleInputStream inputStream;
    private final ArrayBackedValueStorage resultStorage;
    private final DataOutput out;

    public StringJsonParseEval(IEvaluatorContext ctx, IScalarEvaluator inputEval, SourceLocation sourceLocation)
            throws IOException {
        this.ctx = ctx;
        this.inputEval = inputEval;
        this.parser = (JSONDataParser) new JSONDataParserFactory()
                .createInputStreamParser(new DefaultExternalRuntimeDataContext(ctx.getTaskContext()));
        this.sourceLocation = sourceLocation;
        inputVal = new VoidPointable();
        utf8Val = new UTF8StringPointable();
        inputStream = new ByteArrayAccessibleInputStream(new byte[0], 0, 0);
        resultStorage = new ArrayBackedValueStorage();
        out = resultStorage.getDataOutput();
        parser.setInputStream(inputStream);
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        inputEval.evaluate(tuple, inputVal);

        if (PointableHelper.checkAndSetMissingOrNull(result, inputVal)) {
            return;
        }

        byte[] bytes = inputVal.getByteArray();
        int offset = inputVal.getStartOffset();
        if (bytes[offset] == ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
            utf8Val.set(bytes, offset + 1, inputVal.getLength() - 1);
            inputStream.setContent(bytes, utf8Val.getCharStartOffset(), utf8Val.getUTF8Length());
            resultStorage.reset();
            try {
                if (parser.parseAnyValue(out)) {
                    result.set(resultStorage);
                    return;
                } else {
                    //Reset the parser: EOF was encountered
                    resetParser();
                }
            } catch (HyracksDataException e) {
                IWarningCollector warningCollector = ctx.getWarningCollector();
                if (warningCollector.shouldWarn()) {
                    warningCollector.warn(Warning.of(sourceLocation, ErrorCode.RECORD_READER_MALFORMED_INPUT_STREAM));
                }
                //Reset the parser: An error was encountered.
                resetParser();
            }
        } else {
            ExceptionUtil.warnTypeMismatch(ctx, sourceLocation, STRING_PARSE_JSON, bytes[offset], 0, ATypeTag.STRING);
        }

        PointableHelper.setNull(result);
    }

    private void resetParser() throws HyracksDataException {
        try {
            parser.reset(inputStream);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }
}
