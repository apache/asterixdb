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

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.WarningCollector;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.FileSystemWatcher;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.NoOpWarningCollector;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * Regression test for AsterixInputStreamReader's handling of an illegal
 * (invalid UTF-8) character byte in the source: previously, CharsetDecoder's
 * CoderResult was never consulted, so the decoder would permanently stall at
 * the bad byte -- never skipping or reporting past it -- until the internal
 * byte buffer saturated and every subsequent read became a non-blocking,
 * CPU-pegging infinite spin. The fix configures the decoder to substitute
 * illegal characters with the Unicode REPLACEMENT CHARACTER (U+FFFD) and
 * keep reading by default, with an explicit fail-fast option that reports
 * the error immediately as an EXTERNAL_SOURCE_ERROR instead of stalling.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_SONNET_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Regression test for illegal-character handling (replace-by-default, fail-fast option) in "
        + "AsterixInputStreamReader")
public class AsterixInputStreamReaderIllegalCharacterTest {

    private Path tempFile;

    @After
    public void cleanup() throws IOException {
        if (tempFile != null) {
            Files.deleteIfExists(tempFile);
        }
    }

    private Path writeIllegalCharacterFile() throws IOException {
        return writeFileWithRawBytes("{\"value\": \"", new int[] { 0xE9 }, "hello\"}\n");
    }

    /**
     * Writes {@code prefix} and {@code suffix} as plain ASCII, with {@code rawBytes} spliced in between exactly as
     * given (not encoded) -- lets a test embed a specific, possibly-invalid, UTF-8 byte sequence at a known position.
     */
    private Path writeFileWithRawBytes(String prefix, int[] rawBytes, String suffix) throws IOException {
        Path file = Files.createTempFile("illegal-character-utf8", ".json");
        try (java.io.OutputStream out = Files.newOutputStream(file)) {
            out.write(prefix.getBytes(StandardCharsets.US_ASCII));
            for (int b : rawBytes) {
                out.write(b);
            }
            out.write(suffix.getBytes(StandardCharsets.US_ASCII));
        }
        return file;
    }

    /**
     * Like {@link #writeFileWithRawBytes}, but for an arbitrary number of illegal-byte occurrences, each surrounded
     * by well-formed text. Each segment is either a {@code String} (written as plain ASCII) or an {@code int}
     * (written as a single raw byte, not encoded) -- e.g. {@code writeFileWithSegments("a", 0x80, "b", 0xFF, "c")}.
     */
    private Path writeFileWithSegments(Object... segments) throws IOException {
        Path file = Files.createTempFile("illegal-character-utf8", ".json");
        try (java.io.OutputStream out = Files.newOutputStream(file)) {
            for (Object segment : segments) {
                if (segment instanceof String) {
                    out.write(((String) segment).getBytes(StandardCharsets.US_ASCII));
                } else {
                    out.write((int) segment);
                }
            }
        }
        return file;
    }

    private AsterixInputStreamReader createReader(Path file, boolean failOnIllegalCharacter) throws Exception {
        return createReader(file, failOnIllegalCharacter, NoOpWarningCollector.INSTANCE);
    }

    private AsterixInputStreamReader createReader(Path file, boolean failOnIllegalCharacter,
            org.apache.hyracks.api.exceptions.IWarningCollector warnings) throws Exception {
        FileSystemWatcher watcher = new FileSystemWatcher(Collections.singletonList(file), null, false);
        LocalFSInputStream in = new LocalFSInputStream(watcher);
        return new AsterixInputStreamReader(in, ExternalDataConstants.DEFAULT_BUFFER_SIZE, failOnIllegalCharacter,
                warnings);
    }

    /**
     * Drains a reader to the end via repeated {@code read(char[])} calls. A single call is not always enough: a
     * truncated multi-byte sequence sitting at the very tail of the file is only recognized once the underlying
     * stream reports true EOF, which happens on a later call than the one that returned the well-formed prefix
     * before it.
     */
    private String readAll(AsterixInputStreamReader reader) throws IOException {
        StringBuilder sb = new StringBuilder();
        char[] buf = new char[ExternalDataConstants.DEFAULT_BUFFER_SIZE];
        int len;
        while ((len = reader.read(buf)) != -1) {
            sb.append(buf, 0, len);
        }
        return sb.toString();
    }

    /**
     * The number of *distinct* warnings actually collected. {@code WarningCollector} stores warnings in a
     * {@code Set} keyed by message text ({@code Warning.equals()}/{@code hashCode()} use only the message), and
     * every illegal-character warning from a given reader has an identical message -- {@code
     * AsterixInputStreamReader} has no per-occurrence position info (line/field) to include, unlike other parsers
     * in this codebase. So no matter how many illegal characters are replaced in one file, at most one distinct
     * warning survives -- this is what a real caller (e.g. the HTTP response's "warnings" array) actually
     * receives. {@code WarningCollector.getTotalWarningsCount()} is a different thing entirely: a running count of
     * calls to {@code shouldWarn()}, unrelated to how many distinct warnings ended up in the set.
     */
    private int distinctWarningCount(WarningCollector warnings) {
        List<Warning> collected = new ArrayList<>();
        warnings.getWarnings(collected, Long.MAX_VALUE);
        return collected.size();
    }

    @Test(timeout = 15000)
    public void testDefaultReplacesIllegalCharacterAndContinues() throws Exception {
        tempFile = writeIllegalCharacterFile();
        WarningCollector warnings = new WarningCollector();
        try (AsterixInputStreamReader reader = createReader(tempFile, false, warnings)) {
            char[] buf = new char[ExternalDataConstants.DEFAULT_BUFFER_SIZE];
            int len = reader.read(buf);

            Assert.assertEquals("{\"value\": \"\uFFFDhello\"}\n", new String(buf, 0, len));
            Assert.assertEquals("expected exactly one distinct warning for the one illegal character", 1,
                    distinctWarningCount(warnings));
        }
    }

    @Test(timeout = 15000)
    public void testMultipleIndependentIllegalBytesEachGetOwnReplacement() throws Exception {
        // three separate lone continuation bytes -- each is its own illegal byte, not one multi-byte sequence.
        // Case 1: back-to-back with no good data between them. Case 2: each separated by well-formed text --
        // proves replacement happens independently at each occurrence, in the right place, not just for one
        // contiguous run of bad bytes. Each occurrence still calls shouldWarn() (see getTotalWarningsCount() in
        // WarningCollector), but since AsterixInputStreamReader's warning message never varies by occurrence, all
        // three collapse into a single distinct warning -- see distinctWarningCount().
        tempFile = writeFileWithRawBytes("{\"value\": \"", new int[] { 0x80, 0x80, 0x80 }, "\"}\n");
        WarningCollector warnings = new WarningCollector();
        try (AsterixInputStreamReader reader = createReader(tempFile, false, warnings)) {
            char[] buf = new char[ExternalDataConstants.DEFAULT_BUFFER_SIZE];
            int len = reader.read(buf);

            Assert.assertEquals("{\"value\": \"\uFFFD\uFFFD\uFFFD\"}\n", new String(buf, 0, len));
            Assert.assertEquals(
                    "all three occurrences share an identical message, so only one distinct warning " + "survives", 1,
                    distinctWarningCount(warnings));
        }

        tempFile = writeFileWithSegments("{\"a\": \"", 0x80, "\", \"b\": \"", 0xFF, "\", \"c\": \"", 0xC1, "\"}\n");
        warnings = new WarningCollector();
        try (AsterixInputStreamReader reader = createReader(tempFile, false, warnings)) {
            char[] buf = new char[ExternalDataConstants.DEFAULT_BUFFER_SIZE];
            int len = reader.read(buf);

            Assert.assertEquals("{\"a\": \"\uFFFD\", \"b\": \"\uFFFD\", \"c\": \"\uFFFD\"}\n", new String(buf, 0, len));
            Assert.assertEquals("still one distinct warning even though the occurrences are at separate positions", 1,
                    distinctWarningCount(warnings));
        }
    }

    @Test(timeout = 15000)
    public void testMultiByteMalformedSequenceYieldsSingleReplacementCharacter() throws Exception {
        // 0xF0 (a 4-byte sequence lead byte) followed by one valid continuation byte (0x90), then a non-continuation
        // byte -- CharsetDecoder reports this as a single MALFORMED result of length 2 (the "maximal subpart" of the
        // ill-formed sequence per the Unicode Standard's replacement guidance), not two independent errors, so
        // exactly one replacement character is expected, not two
        tempFile = writeFileWithRawBytes("{\"value\": \"hello", new int[] { 0xF0, 0x90 }, "world\"}\n");
        WarningCollector warnings = new WarningCollector();
        try (AsterixInputStreamReader reader = createReader(tempFile, false, warnings)) {
            char[] buf = new char[ExternalDataConstants.DEFAULT_BUFFER_SIZE];
            int len = reader.read(buf);

            Assert.assertEquals("{\"value\": \"hello\uFFFDworld\"}\n", new String(buf, 0, len));
            Assert.assertEquals(
                    "a single malformed sequence must yield a single warning, regardless of its byte length", 1,
                    distinctWarningCount(warnings));
        }
    }

    @Test(timeout = 15000)
    public void testInvalidUtf8EncodedSurrogateIsReplaced() throws Exception {
        // 0xED 0xA0 0x80 is a well-formed-looking 3-byte sequence that decodes to U+D800, a UTF-16 surrogate code
        // point -- surrogates cannot be legally encoded in UTF-8 (RFC 3629), so this is rejected as malformed even
        // though every individual byte looks like a valid continuation byte
        tempFile = writeFileWithRawBytes("{\"value\": \"hello", new int[] { 0xED, 0xA0, 0x80 }, "world\"}\n");
        WarningCollector warnings = new WarningCollector();
        try (AsterixInputStreamReader reader = createReader(tempFile, false, warnings)) {
            char[] buf = new char[ExternalDataConstants.DEFAULT_BUFFER_SIZE];
            int len = reader.read(buf);

            Assert.assertEquals("{\"value\": \"hello\uFFFDworld\"}\n", new String(buf, 0, len));
            Assert.assertEquals(1, distinctWarningCount(warnings));
        }
    }

    @Test(timeout = 15000)
    public void testByteOrderMarkInMiddleOfFileIsNotTreatedAsIllegal() throws Exception {
        // 0xEF 0xBB 0xBF is the UTF-8 encoding of U+FEFF (byte order mark) -- a perfectly well-formed character as
        // far as decoding goes. Stripping a leading BOM is a concern for higher layers (e.g. LineRecordReader),
        // not this reader, so one appearing mid-stream should decode through untouched, with no warning.
        tempFile = writeFileWithRawBytes("{\"value\": \"hello", new int[] { 0xEF, 0xBB, 0xBF }, "world\"}\n");
        WarningCollector warnings = new WarningCollector();
        try (AsterixInputStreamReader reader = createReader(tempFile, false, warnings)) {
            char[] buf = new char[ExternalDataConstants.DEFAULT_BUFFER_SIZE];
            int len = reader.read(buf);

            Assert.assertEquals("{\"value\": \"hello\uFEFFworld\"}\n", new String(buf, 0, len));
            Assert.assertEquals("a mid-stream BOM is well-formed UTF-8, not an illegal character", 0,
                    distinctWarningCount(warnings));
        }
    }

    @Test(timeout = 15000)
    public void testFailOnIllegalCharacterThrowsImmediately() throws Exception {
        tempFile = writeIllegalCharacterFile();
        try (AsterixInputStreamReader reader = createReader(tempFile, true)) {
            reader.read(new char[ExternalDataConstants.DEFAULT_BUFFER_SIZE]);
            Assert.fail("expected an EXTERNAL_SOURCE_ERROR for the illegal character");
        } catch (HyracksDataException e) {
            // reported immediately as a typed EXTERNAL_SOURCE_ERROR, not stalled
            Assert.assertTrue("expected EXTERNAL_SOURCE_ERROR, got: " + e, e.matches(ErrorCode.EXTERNAL_SOURCE_ERROR));
            Assert.assertTrue("expected the cause to be a CharacterCodingException, got: " + e.getCause(),
                    e.getCause() instanceof CharacterCodingException);
        }
    }

    @Test
    public void testWellFormedFileReadsNormally() throws Exception {
        tempFile = Files.createTempFile("well-formed-utf8", ".json");
        String content = "{\"value\": \"hello world\"}\n";
        Files.write(tempFile, content.getBytes(StandardCharsets.UTF_8));

        try (AsterixInputStreamReader reader = createReader(tempFile, false)) {
            char[] buf = new char[ExternalDataConstants.DEFAULT_BUFFER_SIZE];
            int len = reader.read(buf);

            Assert.assertEquals(content, new String(buf, 0, len));
        }
    }
}
