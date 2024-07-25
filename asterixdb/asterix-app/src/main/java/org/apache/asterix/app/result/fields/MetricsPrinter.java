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
        BUFFERCACHE_PAGEREAD_COUNT("bufferCachePageReadCount");

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

    public MetricsPrinter(ResponseMetrics metrics, Charset resultCharset) {
        this.metrics = metrics;
        this.resultCharset = resultCharset;
    }

    @Override
    public void print(PrintWriter pw) {
        boolean useAscii = !StandardCharsets.UTF_8.equals(resultCharset)
                && !"μ".contentEquals(resultCharset.decode(resultCharset.encode("μ")));
        pw.print("\t\"");
        pw.print(FIELD_NAME);
        pw.print("\": {\n");
        pw.print("\t");
        ResultUtil.printField(pw, Metrics.ELAPSED_TIME.str(), Duration.formatNanos(metrics.getElapsedTime(), useAscii));
        pw.print("\n\t");
        ResultUtil.printField(pw, Metrics.EXECUTION_TIME.str(),
                Duration.formatNanos(metrics.getExecutionTime(), useAscii));
        pw.print("\n\t");
        ResultUtil.printField(pw, Metrics.COMPILE_TIME.str(), Duration.formatNanos(metrics.getCompileTime(), useAscii));
        pw.print("\n\t");
        ResultUtil.printField(pw, Metrics.QUEUE_WAIT_TIME.str(),
                Duration.formatNanos(metrics.getQueueWaitTime(), useAscii));
        pw.print("\n\t");
        ResultUtil.printField(pw, Metrics.RESULT_COUNT.str(), metrics.getResultCount(), true);
        pw.print("\n\t");
        ResultUtil.printField(pw, Metrics.RESULT_SIZE.str(), metrics.getResultSize(), true);
        pw.print("\n\t");
        final boolean hasErrors = metrics.getErrorCount() > 0;
        final boolean hasWarnings = metrics.getWarnCount() > 0;
        final boolean usedCache = !(Double.isNaN(metrics.getBufferCacheHitRatio()));
        ResultUtil.printField(pw, Metrics.PROCESSED_OBJECTS_COUNT.str(), metrics.getProcessedObjects(),
                usedCache || hasWarnings || hasErrors);
        pw.print("\n");
        if (usedCache) {
            pw.print("\t");
            String pctValue = String.format("%.2f%%", metrics.getBufferCacheHitRatio() * 100);
            ResultUtil.printField(pw, Metrics.BUFFERCACHE_HIT_RATIO.str(), pctValue, true);
            pw.print("\n");
            pw.print("\t");
            ResultUtil.printField(pw, Metrics.BUFFERCACHE_PAGEREAD_COUNT.str(), metrics.getBufferCachePageReadCount(),
                    hasWarnings || hasErrors);
            pw.print("\n");
        }
        if (hasWarnings) {
            pw.print("\t");
            ResultUtil.printField(pw, Metrics.WARNING_COUNT.str(), metrics.getWarnCount(), hasErrors);
            pw.print("\n");
        }
        if (hasErrors) {
            pw.print("\t");
            ResultUtil.printField(pw, Metrics.ERROR_COUNT.str(), metrics.getErrorCount(), false);
            pw.print("\n");
        }
        pw.print("\t}");
    }

    @Override
    public String getName() {
        return FIELD_NAME;
    }
}
