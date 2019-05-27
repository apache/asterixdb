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

package org.apache.asterix.translator;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.AlgebricksAppendable;

public class SessionOutput {
    private final SessionConfig config;

    // Output path for primary execution.
    private final PrintWriter out;
    private StringWriter buffer;
    private PrintWriter bufferedOut;

    private final SessionOutput.ResultDecorator preResultDecorator;
    private final SessionOutput.ResultDecorator postResultDecorator;
    private SessionOutput.ResultAppender handleAppender;
    private final SessionOutput.ResultAppender statusAppender;

    public SessionOutput(PrintWriter out) {
        this(null, out);
    }

    public SessionOutput(SessionConfig config, PrintWriter out) {
        this(config, out, null, null, null, null);
    }

    public SessionOutput(SessionConfig config, PrintWriter out, ResultDecorator preResultDecorator,
            ResultDecorator postResultDecorator, ResultAppender handleAppender, ResultAppender statusAppender) {
        this.config = config;
        this.out = out;
        this.preResultDecorator = preResultDecorator;
        this.postResultDecorator = postResultDecorator;
        this.handleAppender = handleAppender;
        this.statusAppender = statusAppender;
    }

    /**
     * Retrieve the PrintWriter to produce output to.
     */
    public PrintWriter out() {
        return this.bufferedOut != null ? this.bufferedOut : this.out;
    }

    public void setHandleAppender(SessionOutput.ResultAppender handleAppender) {
        this.handleAppender = handleAppender;
    }

    /**
     * buffer the data provided to the PrintWriter returned by out() to be able to set the status of the response
     * message when it can be determined. This is a no-op, if data is already buffered.
     */
    public void hold() {
        if (this.bufferedOut == null) {
            this.buffer = new StringWriter();
            this.bufferedOut = new PrintWriter(this.buffer);
        }
    }

    /**
     * release the data that was buffered by calling hold() and remove the buffer from the pipeline.
     * This is a no-op, if data is not buffered.
     */
    public void release() {
        if (this.bufferedOut != null) {
            this.bufferedOut.flush();
            this.out.write(buffer.toString());
            this.bufferedOut = null;
            this.buffer = null;
        }
    }

    public AlgebricksAppendable resultPrefix(AlgebricksAppendable app) throws AlgebricksException {
        return this.preResultDecorator != null ? this.preResultDecorator.append(app) : app;
    }

    public AlgebricksAppendable resultPostfix(AlgebricksAppendable app) throws AlgebricksException {
        return this.postResultDecorator != null ? this.postResultDecorator.append(app) : app;
    }

    public AlgebricksAppendable appendHandle(AlgebricksAppendable app, String handle) throws AlgebricksException {
        return this.handleAppender != null ? this.handleAppender.append(app, handle) : app;
    }

    public AlgebricksAppendable appendStatus(AlgebricksAppendable app, String status) throws AlgebricksException {
        return this.statusAppender != null ? this.statusAppender.append(app, status) : app;
    }

    public SessionConfig config() {
        return config;
    }

    @FunctionalInterface
    public interface ResultDecorator {
        AlgebricksAppendable append(AlgebricksAppendable app) throws AlgebricksException;
    }

    @FunctionalInterface
    public interface ResultAppender {
        AlgebricksAppendable append(AlgebricksAppendable app, String str) throws AlgebricksException;
    }
}
