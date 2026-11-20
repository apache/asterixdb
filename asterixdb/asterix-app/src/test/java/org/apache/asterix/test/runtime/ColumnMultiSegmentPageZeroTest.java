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
package org.apache.asterix.test.runtime;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import org.apache.asterix.test.common.TestExecutor;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * The column collapse and promotion cases with collapse turned on, and page zero forced into many segments.
 * <p>
 * Page zero spills into further segments once a component has more columns than
 * {@code storage.max.columns.in.zeroth.segment} -- 5000 in production -- and a wide index is exactly the component
 * that does. A zeroth segment of eight columns puts every component these cases build across several segments.
 * <p>
 * The cases alone cannot show that a node collapsed or a key was promoted, or that a merge succeeded: a failed
 * merge leaves the original components in place, so every query still answers correctly. So after the cases run, the
 * storage log is checked: the NCs run in this JVM and storage loggers write {@code target/storage_debug.log}
 * (log4j2-asterixdb-test.xml), which records every merge's outcome.
 */
@RunWith(Parameterized.class)
public class ColumnMultiSegmentPageZeroTest extends SqlppExecutionTest {
    private static final String CONFIG_FILE_NAME = "src/test/resources/cc-multisegment.conf";
    private static final Path STORAGE_LOG = Paths.get("target", "storage_debug.log");
    private static final String COLLAPSE = "collapsing an object node";
    private static final String PROMOTION = "promoting a key out of a collapsed object node";
    private static final Pattern FINISHED_MERGE =
            Pattern.compile("\\[MERGE:[^\\]]*\\].*Finished columnar bulkloader.*maxPageZeroSegments: (\\d+)");
    private static final Pattern FAILED_MERGE = Pattern.compile("MERGE operation .* failed");
    private static long logOffset;
    private static long logStartMillis;

    @BeforeClass
    public static void setUp() throws Exception {
        logOffset = Files.exists(STORAGE_LOG) ? Files.size(STORAGE_LOG) : 0;
        logStartMillis = System.currentTimeMillis();
        TestExecutor testExecutor = new TestExecutor();
        LangExecutionUtil.setUp(CONFIG_FILE_NAME, testExecutor);
        setNcEndpoints(testExecutor);
    }

    /** Runs before the cluster is torn down, so background merges started by the last case can still finish. */
    @AfterClass
    public static void collapseAndMergesActuallyHappened() throws Exception {
        String log;
        long deadline = System.currentTimeMillis() + 60_000;
        do {
            log = readLogSince(STORAGE_LOG, logOffset);
            if (FINISHED_MERGE.matcher(log).find()) {
                break;
            }
            Thread.sleep(500);
        } while (System.currentTimeMillis() < deadline);

        assertEquals("a failed merge leaves the original components behind and every query still passes -- the "
                + "only sign is the log", 0, countMatches(FAILED_MERGE.matcher(log)));
        int widest = 0;
        Matcher merge = FINISHED_MERGE.matcher(log);
        while (merge.find()) {
            widest = Math.max(widest, Integer.parseInt(merge.group(1)));
        }
        assertTrue("merges must have written page zero across more than one segment, or this suite tested the "
                + "single-segment path again", widest >= 1);

        // The same blind spot, for the write side: a case that never collapses its node, or never promotes, still
        // answers every query correctly. org.apache.asterix.column logs to the storage log too, without additivity, so
        // the same text holds them.
        assertTrue("no object node collapsed, so these cases tested nothing about the collapsed representation",
                count(log, COLLAPSE) > 0);
        assertTrue("no key was promoted, so promote-collapsed-key tested reads of a key that never moved",
                count(log, PROMOTION) > 0);
    }

    private static String readLogSince(Path log, long offset) throws Exception {
        /*
         * The storage log rolls over every 10MB (log4j2-asterixdb-test.xml), and these cases log enough to cross that
         * mid-run. Whatever rolled since setUp is in the archives: the oldest of them is the file setUp measured, so
         * its first `offset` bytes predate this run, and after a roll the live file is entirely this run's.
         */
        List<Path> archives = new ArrayList<>();
        try (DirectoryStream<Path> rolled = Files.newDirectoryStream(log.toAbsolutePath().getParent(),
                log.getFileName().toString().replace(".log", "") + "-*.log.gz")) {
            for (Path archive : rolled) {
                if (Files.getLastModifiedTime(archive).toMillis() >= logStartMillis) {
                    archives.add(archive);
                }
            }
        }
        archives.sort(Comparator.comparingLong(archive -> {
            try {
                return Files.getLastModifiedTime(archive).toMillis();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }));
        StringBuilder text = new StringBuilder();
        long skip = offset;
        for (Path archive : archives) {
            try (InputStream in = new GZIPInputStream(Files.newInputStream(archive))) {
                byte[] bytes = in.readAllBytes();
                int start = (int) Math.min(skip, bytes.length);
                text.append(new String(bytes, start, bytes.length - start, StandardCharsets.UTF_8));
                skip = 0;
            }
        }
        if (!Files.exists(log)) {
            return text.toString();
        }
        try (RandomAccessFile file = new RandomAccessFile(log.toFile(), "r")) {
            long start = Math.min(skip, file.length());
            byte[] bytes = new byte[(int) (file.length() - start)];
            file.seek(start);
            file.readFully(bytes);
            return text.append(new String(bytes, StandardCharsets.UTF_8)).toString();
        }
    }

    private static int count(String text, String needle) {
        int n = 0;
        for (int i = text.indexOf(needle); i >= 0; i = text.indexOf(needle, i + needle.length())) {
            n++;
        }
        return n;
    }

    private static int countMatches(Matcher matcher) {
        int n = 0;
        while (matcher.find()) {
            n++;
        }
        return n;
    }

    @Parameters(name = "ColumnMultiSegmentPageZeroTest {index}: {0}")
    public static Collection<Object[]> tests() throws Exception {
        return LangExecutionUtil.tests("only_sqlpp.xml", "testsuite_column_multisegment.xml");
    }

    public ColumnMultiSegmentPageZeroTest(TestCaseContext tcCtx) {
        super(tcCtx);
    }
}
