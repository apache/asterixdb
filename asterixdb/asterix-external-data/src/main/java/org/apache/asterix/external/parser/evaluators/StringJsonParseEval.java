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
import java.nio.charset.StandardCharsets;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.parser.JSONDataParser;
import org.apache.asterix.external.parser.factory.JSONDataParserFactory;
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
import org.apache.hyracks.util.LogRedactionUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class StringJsonParseEval implements IScalarEvaluator {
    private static final Logger LOGGER = LogManager.getLogger();
    private final IEvaluatorContext ctx;
    private final IScalarEvaluator inputEval;
    private final JSONDataParser parser;
    private final SourceLocation sourceLocation;
    private final IPointable inputVal;
    private final UTF8StringPointable utf8Val;
    private final ByteArrayAccessibleInputStream inputStream;
    private final ArrayBackedValueStorage resultStorage;
    private final DataOutput out;

    //@AiProvenance(agent = AiProvenance.Agent.CLAUDE_SONNET_4_6, tool = AiProvenance.Tool.GITHUB_COPILOT, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Three-outcome enum to distinguish parse success, EOF, and error in tryParseAndSetResult")
    private enum ParseOutcome {
        SUCCESS,
        EOF,
        ERROR
    }

    public StringJsonParseEval(IEvaluatorContext ctx, IScalarEvaluator inputEval, SourceLocation sourceLocation)
            throws IOException {
        this.ctx = ctx;
        this.inputEval = inputEval;
        this.parser = (JSONDataParser) new JSONDataParserFactory().createInputStreamParser(ctx.getTaskContext(), 0);
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
            ParseOutcome outcome = tryParseAndSetResult(result);
            if (outcome == ParseOutcome.SUCCESS) {
                return;
            }
            resetParser();
            if (outcome == ParseOutcome.ERROR
                    && containsCesu8Surrogate(bytes, utf8Val.getCharStartOffset(), utf8Val.getUTF8Length())) {
                // AsterixDB stores strings in CESU-8 (surrogate pairs encoded as two 3-byte sequences).
                // Jackson 2.20+ rejects raw surrogate bytes in UTF-8 streams per RFC 3629.
                // If the failure looks like a surrogate encoding issue, decode via CESU-8 to a Java
                // String, re-encode as proper UTF-8, and retry once before treating as a real error.
                byte[] utf8Bytes = utf8Val.toString().getBytes(StandardCharsets.UTF_8);
                inputStream.setContent(utf8Bytes, 0, utf8Bytes.length);
                outcome = tryParseAndSetResult(result);
                if (outcome == ParseOutcome.SUCCESS) {
                    return;
                }
                resetParser();
            }
            if (outcome == ParseOutcome.ERROR) {
                IWarningCollector warningCollector = ctx.getWarningCollector();
                if (warningCollector.shouldWarn()) {
                    warningCollector.warn(Warning.of(sourceLocation, ErrorCode.RECORD_READER_MALFORMED_INPUT_STREAM));
                }
            }
        } else {
            ExceptionUtil.warnTypeMismatch(ctx, sourceLocation, STRING_PARSE_JSON, bytes[offset], 0, ATypeTag.STRING);
        }

        PointableHelper.setNull(result);
    }

    /**
     * Attempts to parse the current inputStream content.
     * Returns {@link ParseOutcome#SUCCESS} and sets {@code result} on success,
     * {@link ParseOutcome#EOF} if the input was empty, or {@link ParseOutcome#ERROR} on a parse failure.
     */
    //@AiProvenance(agent = AiProvenance.Agent.CLAUDE_SONNET_4_6, tool = AiProvenance.Tool.GITHUB_COPILOT, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Extracted to eliminate duplicated try/catch parse blocks; returns ParseOutcome to preserve distinct EOF vs error semantics")
    private ParseOutcome tryParseAndSetResult(IPointable result) throws HyracksDataException {
        resultStorage.reset();
        try {
            if (parser.parseAnyValue(out)) {
                result.set(resultStorage);
                return ParseOutcome.SUCCESS;
            }
            return ParseOutcome.EOF;
        } catch (HyracksDataException e) {
            LOGGER.debug("failed to parse json value: {}", LogRedactionUtil.userData(e.toString()));
            return ParseOutcome.ERROR;
        }
    }

    private void resetParser() throws HyracksDataException {
        try {
            parser.reset(inputStream);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    /**
     * Returns true if the byte range contains a CESU-8 encoded surrogate (0xED [0xA0-0xBF] ...).
     * Such sequences are valid CESU-8 but invalid UTF-8, and are rejected by Jackson 2.20+.
     * Scanning for 0xED is cheap and covers the vast majority of inputs with zero allocation.
     */
    //@AiProvenance(agent = AiProvenance.Agent.CLAUDE_SONNET_4_6, tool = AiProvenance.Tool.GITHUB_COPILOT, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Fast pre-scan to detect CESU-8 surrogates (0xED [0xA0-0xBF]) before triggering the more expensive CESU-8 to UTF-8 re-encoding retry path")
    private static boolean containsCesu8Surrogate(byte[] bytes, int offset, int length) {
        int end = offset + length;
        for (int i = offset; i < end - 1; i++) {
            if ((bytes[i] & 0xFF) == 0xED && (bytes[i + 1] & 0xF0) == 0xA0) {
                return true;
            }
        }
        return false;
    }
}
