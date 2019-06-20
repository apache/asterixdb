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
package org.apache.asterix.app.result;

import static org.apache.hyracks.util.StorageUtil.StorageUnit.KILOBYTE;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import org.apache.asterix.api.http.server.ResultUtil;
import org.apache.asterix.app.result.fields.ErrorsPrinter;
import org.apache.asterix.app.result.fields.MetricsPrinter;
import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.config.CompilerProperties;
import org.apache.asterix.test.common.ResultExtractor;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.asterix.translator.SessionConfig;
import org.apache.asterix.translator.SessionOutput;
import org.apache.commons.io.IOUtils;
import org.apache.hyracks.util.StorageUtil;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class ResultPrinterTest {

    /**
     * Ensures that a valid JSON is returned when an exception is encountered
     * while printing results and that the errors field is returned in the
     * presence of results fields.
     *
     * @throws Exception
     */
    @Test
    public void exceptionWhilePrinting() throws Exception {
        final RuntimeException expectedException = new RuntimeException("Error reading result");
        final IApplicationContext appCtx = Mockito.mock(IApplicationContext.class);
        final CompilerProperties compilerProperties = Mockito.mock(CompilerProperties.class);
        Mockito.when(appCtx.getCompilerProperties()).thenReturn(compilerProperties);
        Mockito.when(compilerProperties.getFrameSize()).thenReturn(StorageUtil.getIntSizeInBytes(32, KILOBYTE));
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final PrintWriter out = new PrintWriter(baos, true);
        final SessionOutput sessionOutput = createSessionOutput(out);
        final ResultPrinter rs = new ResultPrinter(appCtx, sessionOutput, new IStatementExecutor.Stats(), null);
        final ResultReader resultReader = Mockito.mock(ResultReader.class);
        // throw exception after the resultPrefix is written
        Mockito.when(resultReader.getFrameTupleAccessor()).thenThrow(new RuntimeException(expectedException));
        out.print("{");
        try {
            rs.print(resultReader);
        } catch (RuntimeException e) {
            final ExecutionError error = ExecutionError.of(e);
            new ErrorsPrinter(Collections.singletonList(error)).print(out);
            printMetrics(out, 1);
        }
        out.print("}");
        out.flush();
        final String resultStr = new String(baos.toByteArray(), StandardCharsets.UTF_8);
        boolean exceptionThrown = false;
        try {
            // ensure result is valid json and error will be returned and not results.
            ResultExtractor.extract(IOUtils.toInputStream(resultStr, StandardCharsets.UTF_8), StandardCharsets.UTF_8)
                    .getResult();
        } catch (Exception e) {
            exceptionThrown = true;
            Assert.assertTrue(e.getMessage().contains(expectedException.getMessage()));
        }
        Assert.assertTrue(exceptionThrown);
    }

    private static SessionOutput createSessionOutput(PrintWriter resultWriter) {
        SessionOutput.ResultDecorator resultPrefix = ResultUtil.createPreResultDecorator();
        SessionOutput.ResultDecorator resultPostfix = ResultUtil.createPostResultDecorator();
        SessionOutput.ResultAppender appendHandle = ResultUtil.createResultHandleAppender(null);
        SessionOutput.ResultAppender appendStatus = ResultUtil.createResultStatusAppender();
        SessionConfig.OutputFormat format = SessionConfig.OutputFormat.CLEAN_JSON;
        SessionConfig sessionConfig = new SessionConfig(format);
        sessionConfig.set(SessionConfig.FORMAT_WRAPPER_ARRAY, true);
        sessionConfig.set(SessionConfig.FORMAT_INDENT_JSON, true);
        sessionConfig.set(SessionConfig.FORMAT_QUOTE_RECORD,
                format != SessionConfig.OutputFormat.CLEAN_JSON && format != SessionConfig.OutputFormat.LOSSLESS_JSON);
        return new SessionOutput(sessionConfig, resultWriter, resultPrefix, resultPostfix, appendHandle, appendStatus);
    }

    private static void printMetrics(PrintWriter pw, long errorCount) {
        pw.print("\t\"");
        pw.print(MetricsPrinter.FIELD_NAME);
        pw.print("\": {\n");
        ResultUtil.printField(pw, "errorCount", errorCount, false);
        pw.print("\t}\n");
    }
}
