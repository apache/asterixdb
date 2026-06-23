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
package org.apache.asterix.external.input.stream;

import static org.apache.hyracks.api.util.ExceptionUtils.getMessageOrToString;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.charset.StandardCharsets;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.external.api.AsterixInputStream;
import org.apache.asterix.external.dataflow.AbstractFeedDataFlowController;
import org.apache.asterix.external.util.IFeedLogManager;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.NoOpWarningCollector;
import org.apache.hyracks.util.ParseUtil;
import org.apache.hyracks.util.annotations.AiProvenance;

public class AsterixInputStreamReader extends Reader {

    // Unicode REPLACEMENT CHARACTER (U+FFFD) -- the standard substitute for an illegal/unmappable
    // byte sequence during decoding (e.g. WHATWG Encoding Standard, Python's errors='replace').
    private static final char ILLEGAL_CHARACTER_REPLACEMENT = '\uFFFD';

    private final AsterixInputStream in;
    private final CharsetDecoder decoder;
    private final boolean failOnIllegalCharacter;
    private final IWarningCollector warnings;
    private byte[] bytes;
    protected final ByteBuffer byteBuffer;
    protected final CharBuffer charBuffer;
    protected boolean done = false;
    protected boolean remaining = false;

    public AsterixInputStreamReader(AsterixInputStream in, int bufferSize) {
        this(in, bufferSize, false, NoOpWarningCollector.INSTANCE);
    }

    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_SONNET_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Illegal (invalid UTF-8) characters previously stalled the decoder forever since its "
            + "CoderResult was never consulted; default to substituting U+FFFD and continuing (with a warning), "
            + "with an explicit fail-fast option that reports the error immediately instead of stalling")
    public AsterixInputStreamReader(AsterixInputStream in, int bufferSize, boolean failOnIllegalCharacter,
            IWarningCollector warnings) {
        this.in = in;
        this.failOnIllegalCharacter = failOnIllegalCharacter;
        this.warnings = warnings;
        // Always left at the default REPORT action for both malformed input and unmappable characters: the
        // replace-and-continue path below is handled manually so a warning can be raised at the exact point of
        // substitution, which CharsetDecoder's own CodingErrorAction.REPLACE does not surface.
        this.decoder = StandardCharsets.UTF_8.newDecoder();
        this.bytes = new byte[bufferSize];
        this.byteBuffer = ByteBuffer.wrap(bytes);
        this.charBuffer = CharBuffer.allocate(bufferSize);
        this.byteBuffer.flip();
    }

    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_SONNET_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Consult the decoder's CoderResult (previously discarded). In fail-fast mode, report an illegal "
            + "character as EXTERNAL_SOURCE_ERROR instead of the generic untyped wrapping; otherwise skip past it, "
            + "substitute U+FFFD, warn, and keep decoding the rest of the buffer")
    private void decode(boolean endOfInput) throws IOException {
        while (true) {
            CoderResult result = decoder.decode(byteBuffer, charBuffer, endOfInput);
            if (!result.isError()) {
                return;
            }
            if (failOnIllegalCharacter) {
                try {
                    result.throwException();
                } catch (CharacterCodingException e) {
                    throw RuntimeDataException.create(ErrorCode.EXTERNAL_SOURCE_ERROR, e, getMessageOrToString(e));
                }
            }
            if (!charBuffer.hasRemaining()) {
                // No room for the replacement character right now. byteBuffer's position is unchanged, so the
                // caller will flush charBuffer, refill it, and the same error will be reported again on retry.
                return;
            }
            reportIllegalCharacter();
            charBuffer.put(ILLEGAL_CHARACTER_REPLACEMENT);
            byteBuffer.position(byteBuffer.position() + result.length());
        }
    }

    private void reportIllegalCharacter() {
        if (warnings.shouldWarn()) {
            // fieldNum has no "not applicable" sentinel (unlike lineNum, negative values are not hidden), so pass 0
            // to match the convention used by other ParseUtil.warn() callers in this package
            ParseUtil.warn(warnings, in.getStreamName(), -1, 0,
                    "illegal character encountered; substituted with the Unicode replacement character (U+FFFD)");
        }
    }

    public void stop() throws IOException {
        try {
            in.stop();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public void setController(AbstractFeedDataFlowController controller) {
        in.setController(controller);
    }

    public void setFeedLogManager(IFeedLogManager feedLogManager) throws HyracksDataException {
        in.setFeedLogManager(feedLogManager);
    }

    @Override
    public int read(char cbuf[]) throws IOException {
        return read(cbuf, 0, cbuf.length);
    }

    @Override
    public int read(char[] cbuf, int offset, int length) throws IOException {
        if (done) {
            return -1;
        }
        int len = 0;
        charBuffer.clear();
        while (charBuffer.position() == 0) {
            if (byteBuffer.hasRemaining()) {
                remaining = true;
                decode(false);
                System.arraycopy(charBuffer.array(), 0, cbuf, offset, charBuffer.position());
                if (charBuffer.position() > 0) {
                    return charBuffer.position();
                } else {
                    // need to read more data
                    System.arraycopy(bytes, byteBuffer.position(), bytes, 0, byteBuffer.remaining());
                    len = 0; // reset to read more bytes
                    byteBuffer.position(byteBuffer.remaining());
                    byteBuffer.limit(byteBuffer.capacity()); //set limit to capacity for the new bytes
                    while (len == 0) {
                        checkInterrupted();
                        len = in.read(bytes, byteBuffer.position(), bytes.length - byteBuffer.position());
                    }
                }
            } else {
                byteBuffer.clear();
                while (len == 0) {
                    checkInterrupted();
                    len = in.read(bytes, 0, bytes.length);
                }
            }
            if (len == -1) {
                done = true;
                return len;
            }
            if (remaining) {
                byteBuffer.position(len + byteBuffer.position());
            } else {
                byteBuffer.position(len);
            }
            byteBuffer.flip();
            remaining = false;
            decode(false);
            System.arraycopy(charBuffer.array(), 0, cbuf, offset, charBuffer.position());
        }
        return charBuffer.position();
    }

    @AiProvenance(agent = AiProvenance.Agent.DEEPSEEK_CODER, tool = AiProvenance.Tool.FACTORY_CLI, contributionKind = AiProvenance.ContributionKind.ASSISTED, notes = "Check for thread interruption (and throw InterruptedIOException) to prevent infinite spin in read loop when stream is cancelled and the inputStream is not handling interrupts")
    private static void checkInterrupted() throws IOException {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedIOException("Thread interrupted while reading stream");
        }
    }

    @Override
    public void close() throws IOException {
        in.close();
    }

    public boolean handleException(Throwable th) {
        return in.handleException(th);
    }

    @Override
    public void reset() throws IOException {
        byteBuffer.limit(0);
    }

    public String getStreamName() {
        return in.getStreamName();
    }

    public String getPreviousStreamName() {
        return in.getPreviousStreamName();
    }
}
