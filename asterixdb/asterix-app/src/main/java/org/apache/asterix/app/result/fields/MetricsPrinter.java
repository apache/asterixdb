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
package org.apache.asterix.app.result.fields;

import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.asterix.api.http.server.ResultUtil;
import org.apache.asterix.app.result.ResponseMetrics;
import org.apache.asterix.common.api.Duration;
import org.apache.asterix.common.api.IResponseFieldPrinter;

public class MetricsPrinter implements IResponseFieldPrinter {

    public enum Metrics {
        ELAPSED_TIME("elapsedTime"),
        EXECUTION_TIME("executionTime"),
        COMPILE_TIME("compileTime"),
        QUEUE_WAIT_TIME("queueWaitTime"),
        RESULT_COUNT("resultCount"),
        RESULT_SIZE("resultSize"),
        ERROR_COUNT("errorCount"),
        PROCESSED_OBJECTS_COUNT("processedObjects"),
        WARNING_COUNT("warningCount"),
        BUFFERCACHE_HIT_RATIO("bufferCacheHitRatio"),
        BUFFERCACHE_PAGEREAD_COUNT("bufferCachePageReadCount"),
        REMOTE_STORAGE_REQUESTS_COUNT("remoteStorageRequestsCount"),
        REMOTE_STORAGE_PAGES_READ_COUNT("remoteStoragePagesReadCount"),
        REMOTE_PAGES_PERSISTED_COUNT("remoteStoragePagesPersistedCount");

        private final String str;

        Metrics(String str) {
            this.str = str;
        }

        public String str() {
            return str;
        }
    }

    public static final String FIELD_NAME = "metrics";
    private final ResponseMetrics metrics;
    private final Charset resultCharset;
    private final Set<Metrics> selectedMetrics;

    public MetricsPrinter(ResponseMetrics metrics, Charset resultCharset) {
        this(metrics, resultCharset, null);
    }

    public MetricsPrinter(ResponseMetrics metrics, Charset resultCharset, Set<Metrics> selectedMetrics) {
        this.metrics = metrics;
        this.resultCharset = resultCharset;
        this.selectedMetrics = selectedMetrics == null ? Set.of(Metrics.values()) : selectedMetrics;
    }

    @Override
    public void print(PrintWriter pw) {
        boolean useAscii = !StandardCharsets.UTF_8.equals(resultCharset)
                && !"μ".contentEquals(resultCharset.decode(resultCharset.encode("μ")));

        final boolean hasErrors = metrics.getErrorCount() > 0;
        final boolean hasWarnings = metrics.getWarnCount() > 0;
        final boolean usedCache = !(Double.isNaN(metrics.getBufferCacheHitRatio()));
        final boolean madeCloudReadRequests = metrics.getCloudReadRequestsCount() > 0;

        // Entry representation with typed value
        abstract class Entry {
            final Metrics m;

            Entry(Metrics m) {
                this.m = m;
            }

            abstract void print(PrintWriter out, boolean more);
        }
        class StringEntry extends Entry {
            final String v;

            StringEntry(Metrics m, String v) {
                super(m);
                this.v = v;
            }

            @Override
            void print(PrintWriter out, boolean more) {
                ResultUtil.printField(out, m.str(), v, more);
            }
        }
        class LongEntry extends Entry {
            final long v;

            LongEntry(Metrics m, long v) {
                super(m);
                this.v = v;
            }

            @Override
            void print(PrintWriter out, boolean more) {
                ResultUtil.printField(out, m.str(), v, more);
            }
        }

        List<Entry> entries = new ArrayList<>();

        if (isSelected(Metrics.ELAPSED_TIME))
            entries.add(
                    new StringEntry(Metrics.ELAPSED_TIME, Duration.formatNanos(metrics.getElapsedTime(), useAscii)));
        if (isSelected(Metrics.EXECUTION_TIME))
            entries.add(new StringEntry(Metrics.EXECUTION_TIME,
                    Duration.formatNanos(metrics.getExecutionTime(), useAscii)));
        if (isSelected(Metrics.COMPILE_TIME))
            entries.add(
                    new StringEntry(Metrics.COMPILE_TIME, Duration.formatNanos(metrics.getCompileTime(), useAscii)));
        if (isSelected(Metrics.QUEUE_WAIT_TIME))
            entries.add(new StringEntry(Metrics.QUEUE_WAIT_TIME,
                    Duration.formatNanos(metrics.getQueueWaitTime(), useAscii)));
        if (isSelected(Metrics.RESULT_COUNT))
            entries.add(new LongEntry(Metrics.RESULT_COUNT, metrics.getResultCount()));
        if (isSelected(Metrics.RESULT_SIZE))
            entries.add(new LongEntry(Metrics.RESULT_SIZE, metrics.getResultSize()));
        if (isSelected(Metrics.PROCESSED_OBJECTS_COUNT))
            entries.add(new LongEntry(Metrics.PROCESSED_OBJECTS_COUNT, metrics.getProcessedObjects()));

        if (usedCache) {
            if (isSelected(Metrics.BUFFERCACHE_HIT_RATIO)) {
                String pctValue = String.format("%.2f%%", metrics.getBufferCacheHitRatio() * 100);
                entries.add(new StringEntry(Metrics.BUFFERCACHE_HIT_RATIO, pctValue));
            }
            if (isSelected(Metrics.BUFFERCACHE_PAGEREAD_COUNT)) {
                entries.add(new LongEntry(Metrics.BUFFERCACHE_PAGEREAD_COUNT, metrics.getBufferCachePageReadCount()));
            }
        }

        if (madeCloudReadRequests) {
            if (isSelected(Metrics.REMOTE_STORAGE_REQUESTS_COUNT)) {
                entries.add(new LongEntry(Metrics.REMOTE_STORAGE_REQUESTS_COUNT, metrics.getCloudReadRequestsCount()));
            }
            if (isSelected(Metrics.REMOTE_STORAGE_PAGES_READ_COUNT)) {
                entries.add(new LongEntry(Metrics.REMOTE_STORAGE_PAGES_READ_COUNT, metrics.getCloudPagesReadCount()));
            }
            if (isSelected(Metrics.REMOTE_PAGES_PERSISTED_COUNT)) {
                entries.add(new LongEntry(Metrics.REMOTE_PAGES_PERSISTED_COUNT, metrics.getCloudPagesPersistedCount()));
            }
        }

        if (hasWarnings && isSelected(Metrics.WARNING_COUNT)) {
            entries.add(new LongEntry(Metrics.WARNING_COUNT, metrics.getWarnCount()));
        }
        if (hasErrors && isSelected(Metrics.ERROR_COUNT)) {
            entries.add(new LongEntry(Metrics.ERROR_COUNT, metrics.getErrorCount()));
        }

        pw.print("\t\"");
        pw.print(FIELD_NAME);
        pw.print("\": {\n");
        for (int i = 0; i < entries.size(); i++) {
            Entry e = entries.get(i);
            boolean hasMore = i < entries.size() - 1;
            pw.print("\t");
            e.print(pw, hasMore);
            pw.print("\n");
        }
        pw.print("\t}");
    }

    private boolean isSelected(Metrics m) {
        return selectedMetrics.contains(m);
    }

    @Override
    public String getName() {
        return FIELD_NAME;
    }
}
